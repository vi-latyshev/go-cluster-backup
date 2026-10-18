package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

var _ storage.MultipartUploader = (*Storage)(nil)

// maxParts is the service's own limit on the parts of one multipart upload. An
// object above it can never be committed here, so BeginMultipart refuses it
// before a single part is sent.
const maxParts = 10000

// abortTimeoutDivisor scales the request timeout down for an abort: an abort
// carries no body, so it neither needs nor deserves the time a large request is
// allowed — and it runs exactly when the upload's deadline has just fired, where
// every second it can spend is a second the run's ending needs.
const abortTimeoutDivisor = 10

// BeginMultipart opens one multipart upload. An object that fits one part is
// answered with errors.ErrUnsupported and no request at all: the engine reads
// that as "use Put", which is the one-request path this plugin stores a small
// object with. An object that would need more parts than the service accepts is
// refused here, permanently and before any request — the alternative is
// uploading it for hours and failing the commit. The part size is this plugin's
// configured part_bytes — the engine slices the object with it and sends every
// part as its own call.
func (s *Storage) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return storage.MultipartSession{}, err
	}
	if req.Size > s.cfg.MaxObjectBytes.Int64() {
		return storage.MultipartSession{}, plugin.Permanent(
			fmt.Errorf("object %s is %d bytes, above the endpoint limit of %d", req.Key, req.Size, s.cfg.MaxObjectBytes))
	}
	if parts := (req.Size + s.cfg.PartBytes.Int64() - 1) / s.cfg.PartBytes.Int64(); parts > maxParts {
		return storage.MultipartSession{}, plugin.Permanent(fmt.Errorf(
			"object %s is %d bytes: %d parts of %d bytes, above the %d parts the service accepts in one upload",
			req.Key, req.Size, parts, s.cfg.PartBytes, int64(maxParts)))
	}
	if req.Size <= s.cfg.PartBytes.Int64() {
		return storage.MultipartSession{}, fmt.Errorf(
			"object %s is %d bytes, one part carries %d: a single Put stores it: %w",
			req.Key, req.Size, s.cfg.PartBytes, errors.ErrUnsupported)
	}
	c, err := s.client()
	if err != nil {
		return storage.MultipartSession{}, err
	}
	full := s.key(req.Key)
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	created, err := c.CreateMultipartUpload(rctx, &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(s.cfg.Bucket),
		Key:          aws.String(full),
		StorageClass: types.StorageClass(s.cfg.StorageClass),
	})
	if err != nil {
		return storage.MultipartSession{}, s.classify(ctx, err, "create multipart upload "+full)
	}
	id := aws.ToString(created.UploadId)
	if id == "" {
		return storage.MultipartSession{}, plugin.Transient(
			fmt.Errorf("create multipart upload %s answered no upload id", full))
	}
	return storage.MultipartSession{Key: req.Key, UploadID: id, PartSize: s.cfg.PartBytes.Int64()}, nil
}

// PutPart sends one part. Sending the same number again replaces it, which is
// exactly what the engine's retry of one part relies on; the body is the reader
// the engine handed over — a section of its spool file, seekable and bounded, so
// no buffer of the part is held here — and the SDK signs the request over that
// section. One attempt: a failed part is the engine's to retry.
func (s *Storage) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if err := ctx.Err(); err != nil {
		return storage.PartResult{}, err
	}
	if part.Number < 1 {
		return storage.PartResult{}, plugin.Permanent(
			fmt.Errorf("part number %d is not 1-based", part.Number))
	}
	if part.Size <= 0 || part.Size > s.cfg.PartBytes.Int64() {
		return storage.PartResult{}, plugin.Permanent(fmt.Errorf(
			"part %d is %d bytes, and a part carries between 1 and %d", part.Number, part.Size, s.cfg.PartBytes))
	}
	c, err := s.client()
	if err != nil {
		return storage.PartResult{}, err
	}
	full := s.key(session.Key)
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	out, err := c.UploadPart(rctx, &s3.UploadPartInput{
		Bucket:        aws.String(s.cfg.Bucket),
		Key:           aws.String(full),
		UploadId:      aws.String(session.UploadID),
		PartNumber:    aws.Int32(part.Number),
		Body:          part.Body,
		ContentLength: aws.Int64(part.Size),
	})
	if err != nil {
		return storage.PartResult{}, s.classify(ctx, err, fmt.Sprintf("upload part %d of %s", part.Number, full))
	}
	etag := aws.ToString(out.ETag)
	if etag == "" {
		return storage.PartResult{}, plugin.Transient(
			fmt.Errorf("upload part %d of %s answered without an ETag: the part cannot be committed", part.Number, full))
	}
	return storage.PartResult{Number: part.Number, ETag: etag}, nil
}

// CompleteMultipart commits the upload. Retrying it is safe: parts the service
// already holds are matched by number and ETag, and an upload that did commit is
// answered "not found" on a second attempt — which the engine reads as the upload
// being gone and settles with its own Stat of the object. A 200 that carries no
// completion ETag is the service's "the upload did not commit" answer and must
// not pass: the engine would otherwise verify a part list that was never
// assembled.
func (s *Storage) CompleteMultipart(ctx context.Context, session storage.MultipartSession, parts []storage.PartResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(parts) == 0 {
		return plugin.Permanent(fmt.Errorf("complete multipart upload %s: no parts were sent", session.Key))
	}
	c, err := s.client()
	if err != nil {
		return err
	}
	full := s.key(session.Key)
	completed := make([]types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(p.Number), ETag: aws.String(p.ETag)})
	}
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	out, err := c.CompleteMultipartUpload(rctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.cfg.Bucket),
		Key:             aws.String(full),
		UploadId:        aws.String(session.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		return s.classify(ctx, err, "complete multipart upload "+full)
	}
	if aws.ToString(out.ETag) == "" {
		return plugin.Transient(fmt.Errorf(
			"complete multipart upload %s answered 200 without a completion ETag: the upload is not committed", full))
	}
	return nil
}

// AbortMultipart discards the upload and every part it holds. The engine calls it
// when the upload failed — including when the context that upload ran under is
// already dead — so the call runs on its own context, detached from that one and
// bounded by request_timeout divided by abortTimeoutDivisor. One attempt is all
// it gets: an abort that fails leaves parts that are billed, which is worth a
// line, never another upload. An upload already gone (NoSuchUpload) is nothing
// left to clean, not an error.
func (s *Storage) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	if session.UploadID == "" {
		return plugin.Permanent(fmt.Errorf("abort multipart upload %s: the session carries no upload id", session.Key))
	}
	c, err := s.client()
	if err != nil {
		return err
	}
	full := s.key(session.Key)
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.RequestTimeout.Std()/abortTimeoutDivisor)
	defer cancel()
	_, err = c.AbortMultipartUpload(actx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.cfg.Bucket),
		Key:      aws.String(full),
		UploadId: aws.String(session.UploadID),
	})
	if err == nil || isNotFound(err) {
		return nil
	}
	return s.classify(actx, err, "abort multipart upload "+full)
}

// StaleMultipart lists the incomplete uploads under this storage's own prefix
// that were initiated before the cutoff. The service answers one page per call;
// the keys it returns are trimmed to this storage's own root exactly like the
// objects List returns, and an upload the service reports outside the prefix is
// skipped rather than handed to the caller — the prefix lookup never returns
// one, and the guard costs nothing.
// Inventory lists every incomplete upload under this storage's own prefix, of
// any age, including one the service does not date: the engine uses it to know
// whether an earlier writer's parts make the storage's occupancy unknown.
func (s *Storage) Inventory(ctx context.Context) ([]storage.StaleUpload, error) {
	return s.listUploads(ctx, nil)
}

func (s *Storage) StaleMultipart(ctx context.Context, olderThan time.Time) ([]storage.StaleUpload, error) {
	return s.listUploads(ctx, &olderThan)
}

// listUploads is the one listing behind StaleMultipart and Inventory: olderThan
// nil is the full inventory; a cutoff selects the uploads old enough to abort,
// and an upload the service does not date is never called stale.
func (s *Storage) listUploads(ctx context.Context, olderThan *time.Time) ([]storage.StaleUpload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := s.client()
	if err != nil {
		return nil, err
	}
	base := s.key("")
	in := &s3.ListMultipartUploadsInput{Bucket: aws.String(s.cfg.Bucket), Prefix: aws.String(base)}
	var stale []storage.StaleUpload
	for {
		rctx, cancel := s.reqCtx(ctx)
		out, err := c.ListMultipartUploads(rctx, in)
		cancel()
		if err != nil {
			return nil, s.classify(ctx, err, "list multipart uploads "+base)
		}
		for _, u := range out.Uploads {
			k := aws.ToString(u.Key)
			if !strings.HasPrefix(k, base) {
				continue // not this storage's key: never listed, never aborted
			}
			t := aws.ToTime(u.Initiated)
			if olderThan != nil {
				// An upload the service does not date cannot be called stale: the
				// sweep aborts, it never guesses, and a missing Initiated would
				// otherwise read as the zero time and take a live upload with it.
				if t.IsZero() || !t.Before(*olderThan) {
					continue
				}
			}
			stale = append(stale, storage.StaleUpload{
				Key: strings.TrimPrefix(k, base), UploadID: aws.ToString(u.UploadId), Initiated: t,
			})
		}
		if !aws.ToBool(out.IsTruncated) {
			return stale, nil
		}
		// Markers that do not move would page forever; the sweep stops with the
		// error rather than spending the storage's budget on the same page.
		nextKey, nextID := aws.ToString(out.NextKeyMarker), aws.ToString(out.NextUploadIdMarker)
		if nextKey == aws.ToString(in.KeyMarker) && nextID == aws.ToString(in.UploadIdMarker) {
			return nil, plugin.Transient(fmt.Errorf("list multipart uploads %s: the markers did not advance", base))
		}
		in.KeyMarker = out.NextKeyMarker
		in.UploadIdMarker = out.NextUploadIdMarker
	}
}
