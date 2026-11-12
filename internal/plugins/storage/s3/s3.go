// Package s3 is the storage plugin over any S3-compatible endpoint.
//
// One attempt per call: the SDK's own retryer is disabled (aws.NopRetryer) and
// every error is classified, so execution owns retries exactly as it does for
// every other storage. Put stores one object in ONE PutObject request; an object
// larger than a part travels part by part through the storage.MultipartUploader
// capability, which transfer drives (see multipart.go) — one part per call and
// no buffer of the object held here, so a part that failed is sent again without
// re-sending the parts the service already accepted.
//
// A write is one PUT and nothing else: the plugin does not verify its own
// object, because transfer stats every copy it uploads and that Stat is the
// proof a copy landed. A 200 that did not commit anything (CompleteMultipartUpload
// may answer 200 with an error in the body) is caught by that Stat, not here.
package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// Addressing styles accepted by the config field.
const (
	AddressingPath    = "path"
	AddressingVirtual = "virtual"
)

// Request checksum modes accepted by the config field: when_required keeps the
// SDK from attaching checksum headers (and aws-chunked framing) that a
// third-party S3 endpoint may not accept; when_supported is the SDK's own
// default behaviour.
const (
	ChecksumWhenRequired  = "when_required"
	ChecksumWhenSupported = "when_supported"
)

// minPartBytes is the S3 minimum for every part but the last: a smaller
// part_bytes would be rejected at Complete time, so it is a config error here.
const minPartBytes = 5 << 20

// storageClassToken is the shape of one payload storage class the endpoint may
// accept. It is deliberately not the AWS enum: an S3-compatible service names
// its own classes (cloud.ru ICE, COLD, SINGLE), and the token is passed through
// exactly as the deployment wrote it. Empty is the other accepted value and
// means "send no class header", which leaves the bucket's default. The regexp is
// built lazily: a package initializer does no work.
var storageClassToken = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
})

type Config struct {
	Endpoint        string               `json:"endpoint" required:"true" doc:"base URL of the S3 endpoint, e.g. https://s3.example.com"`
	Region          string               `json:"region" required:"true" doc:"signing region of the endpoint"`
	Bucket          string               `json:"bucket" required:"true" doc:"bucket every object lives in"`
	Prefix          string               `json:"prefix" required:"true" doc:"key prefix of the storage root; keys are built under it"`
	AccessKeyID     configuration.Secret `json:"access_key_id" doc:"file holding the access key id; an opaque string, e.g. <tenant_id>:<key_id>"`
	SecretAccessKey configuration.Secret `json:"secret_access_key" doc:"file holding the secret access key"`
	Addressing      string               `json:"addressing" required:"true" doc:"addressing style: path or virtual"`
	StorageClass    string               `json:"storage_class" doc:"storage class of artifact objects, exactly as the endpoint names it, e.g. STANDARD or ICE; empty sends no class header and leaves the bucket's default. Run manifests are always STANDARD"`
	RequestChecksum string               `json:"request_checksum" doc:"request checksum mode: when_required or when_supported"`
	PartBytes       model.ByteSize       `json:"part_bytes" doc:"bytes per part of a multipart upload; at least 5 MiB"`
	MaxRequestBytes model.ByteSize       `json:"max_request_bytes" doc:"largest single request the endpoint accepts"`
	MaxObjectBytes  model.ByteSize       `json:"max_object_bytes" doc:"largest object the endpoint accepts"`
	RequestTimeout  model.Duration       `json:"request_timeout" doc:"time until the response headers of one HTTP request; a download body streams under the caller's deadline"`
}

func newStorage(ctx context.Context, cfg Config, _ instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u := cfg.Endpoint
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return nil, fmt.Errorf("endpoint %q must be an absolute http(s) URL", cfg.Endpoint)
	}
	if cfg.Region == "" {
		return nil, errors.New("region is empty")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is empty")
	}
	if strings.HasPrefix(cfg.Prefix, "/") || strings.HasSuffix(cfg.Prefix, "/") {
		return nil, fmt.Errorf("prefix %q must not start or end with %q", cfg.Prefix, "/")
	}
	if cfg.AccessKeyID.File == "" {
		return nil, errors.New("access_key_id file is empty")
	}
	if cfg.SecretAccessKey.File == "" {
		return nil, errors.New("secret_access_key file is empty")
	}
	switch cfg.Addressing {
	case AddressingPath, AddressingVirtual:
	default:
		return nil, fmt.Errorf("addressing %q must be %q or %q", cfg.Addressing, AddressingPath, AddressingVirtual)
	}
	if cfg.StorageClass != "" && !storageClassToken().MatchString(cfg.StorageClass) {
		return nil, fmt.Errorf("storage_class %q must be 1-64 letters, digits, %q or %q, or empty to send no class header",
			cfg.StorageClass, "_", "-")
	}
	var checksum aws.RequestChecksumCalculation
	switch cfg.RequestChecksum {
	case ChecksumWhenRequired:
		checksum = aws.RequestChecksumCalculationWhenRequired
	case ChecksumWhenSupported:
		checksum = aws.RequestChecksumCalculationWhenSupported
	default:
		return nil, fmt.Errorf("request_checksum %q must be %q or %q", cfg.RequestChecksum, ChecksumWhenRequired, ChecksumWhenSupported)
	}
	if cfg.PartBytes.Int64() < minPartBytes {
		return nil, fmt.Errorf("part_bytes %d is below the S3 minimum of %d", cfg.PartBytes.Int64(), int64(minPartBytes))
	}
	// A non-positive max_request_bytes is refused by this comparison already:
	// part_bytes is at least the S3 minimum above, so it can never be below it.
	if cfg.PartBytes.Int64() > cfg.MaxRequestBytes.Int64() {
		return nil, fmt.Errorf("part_bytes %d exceeds max_request_bytes %d: a part is one request", cfg.PartBytes.Int64(), cfg.MaxRequestBytes.Int64())
	}
	if cfg.MaxObjectBytes.Int64() < cfg.MaxRequestBytes.Int64() {
		return nil, fmt.Errorf("max_object_bytes %d is below max_request_bytes %d", cfg.MaxObjectBytes.Int64(), cfg.MaxRequestBytes.Int64())
	}
	if cfg.RequestTimeout <= 0 {
		return nil, fmt.Errorf("request_timeout must be positive, got %s", cfg.RequestTimeout)
	}
	return &Storage{cfg: cfg, checksum: checksum, http: awshttp.NewBuildableClient()}, nil
}

type Storage struct {
	cfg      Config
	checksum aws.RequestChecksumCalculation
	// http is the one client every request of this Storage goes through: its
	// transport owns the connection pool, so the calls of one run reuse
	// connections. It carries no http.Client.Timeout — a timeout there would
	// bound the whole exchange, body included, and undo Get's header-only
	// request_timeout. nil is the SDK's own default; a test substitutes one to
	// hand a request an answer whose body is shorter than the length it
	// declares — the shape Get's own length guard exists for, and one no real
	// transport produces.
	http aws.HTTPClient
}

func (s *Storage) Describe(ctx context.Context) (storage.StorageTraits, error) {
	if err := ctx.Err(); err != nil {
		return storage.StorageTraits{}, err
	}
	return storage.StorageTraits{Checksums: []string{"md5"}}, nil
}

var _ instances.Checker = (*Storage)(nil)

// Check is the instance's preflight: one read-only listing probe at the
// configured bucket and prefix proves the credentials are accepted and the
// bucket is reachable. MaxKeys keeps the probe to at most one key, and an empty
// prefix is not an error — the first run writes into it. A bucket that does not
// exist is a permanent error that names it, as is a rejected credential. One
// attempt: execution retries a transient answer under the configured policy.
func (s *Storage) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := s.client()
	if err != nil {
		return err
	}
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	_, err = c.ListObjectsV2(rctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.cfg.Bucket),
		Prefix:  aws.String(s.key("")),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if st := apiStatus(err); st == http.StatusUnauthorized || st == http.StatusForbidden {
			return pluginerrors.Permanent(fmt.Errorf("check bucket %s: the credentials were rejected: %w", s.cfg.Bucket, err))
		}
		return s.classify(ctx, err, "check bucket "+s.cfg.Bucket)
	}
	return nil
}

// client builds the client for one contract call: the credential files are read
// per call (a rotated key is picked up, a wrong path is a classified error of
// this call rather than a construction failure of the run), and the SDK's
// retryer is off — one attempt per call is the seam's rule.
func (s *Storage) client() (*s3.Client, error) {
	keyID, err := readSecret(s.cfg.AccessKeyID)
	if err != nil {
		return nil, err
	}
	secret, err := readSecret(s.cfg.SecretAccessKey)
	if err != nil {
		return nil, err
	}
	awsCfg := aws.Config{
		Region:                     s.cfg.Region,
		Credentials:                credentials.NewStaticCredentialsProvider(keyID, secret, ""),
		Retryer:                    func() aws.Retryer { return aws.NopRetryer{} },
		RequestChecksumCalculation: s.checksum,
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s.cfg.Endpoint)
		o.UsePathStyle = s.cfg.Addressing == AddressingPath
		o.HTTPClient = s.http // nil leaves the SDK's own client in place
	}), nil
}

func (s *Storage) reqCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.cfg.RequestTimeout.Std())
}

// Put stores one object in one PutObject request. The plugin does not verify its
// own write: transfer's Stat of the object is the one proof a copy landed, and
// a second HEAD here only doubled the requests of every upload. An object larger
// than one request is a permanent error here: transfer uploads such an object
// part by part through the storage.MultipartUploader capability, and Put never
// becomes a multipart upload of its own.
func (s *Storage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	if req.Size > s.cfg.MaxObjectBytes.Int64() {
		return model.ObjectInfo{}, pluginerrors.Permanent(
			fmt.Errorf("object %s is %d bytes, above the endpoint limit of %d", req.Key, req.Size, s.cfg.MaxObjectBytes))
	}
	if req.Size > s.cfg.MaxRequestBytes.Int64() {
		return model.ObjectInfo{}, pluginerrors.Permanent(fmt.Errorf(
			"object %s is %d bytes, above the %d bytes one request carries: it is uploaded part by part through the multipart capability",
			req.Key, req.Size, s.cfg.MaxRequestBytes))
	}
	c, err := s.client()
	if err != nil {
		return model.ObjectInfo{}, err
	}
	full := s.key(req.Key)
	if err := s.putSingle(ctx, c, full, req.Size, s.storageClass(req.Purpose), body); err != nil {
		return model.ObjectInfo{}, err
	}
	return model.ObjectInfo{Key: req.Key, Size: req.Size}, nil
}

// storageClass is the class one request declares. A run manifest is metadata:
// it is always STANDARD, whatever class the artifacts use, because every
// survey, retention pass and restore reads it first and a cold payload class
// must never make that read a retrieval or an InvalidObjectState. An artifact
// gets the configured token; an empty token sends no header at all, which
// leaves the bucket's default in force.
func (s *Storage) storageClass(p storage.Purpose) types.StorageClass {
	if p == storage.PurposeMetadata {
		return types.StorageClassStandard
	}
	return types.StorageClass(s.cfg.StorageClass)
}

// putSingle sends the whole object in one request — the only path that can be
// checked against the request's own md5 later, because a single-part PutObject
// returns the object's md5 as its ETag. A body shorter than ContentLength fails
// the request inside net/http, so the declared size is never a claim the send
// did not honour.
func (s *Storage) putSingle(ctx context.Context, c *s3.Client, full string, size int64, class types.StorageClass, body io.Reader) error {
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	_, err := c.PutObject(rctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.cfg.Bucket),
		Key:           aws.String(full),
		Body:          body,
		ContentLength: aws.Int64(size),
		StorageClass:  class,
	})
	if err != nil {
		return s.classify(ctx, err, "put "+full)
	}
	return nil
}

func (s *Storage) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	c, err := s.client()
	if err != nil {
		return model.ObjectInfo{}, err
	}
	full := s.key(key)
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	out, err := c.HeadObject(rctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(full)})
	if err != nil {
		if isNotFound(err) {
			return model.ObjectInfo{}, pluginerrors.NotFound(fmt.Errorf("object %s is definitely absent", full))
		}
		return model.ObjectInfo{}, s.classify(ctx, err, "head "+full)
	}
	return model.ObjectInfo{Key: key, Size: aws.ToInt64(out.ContentLength), ModTime: aws.ToTime(out.LastModified),
		MD5: etagMD5(out.ETag)}, nil
}

// List returns one service page per call; the service's own default page size
// decides how many objects fit in it, and ContinuationToken carries the rest.
func (s *Storage) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ListPage{}, err
	}
	if strings.HasPrefix(req.Prefix, "/") || strings.Contains(req.Prefix, "..") {
		return storage.ListPage{}, pluginerrors.Permanent(
			fmt.Errorf("prefix %q must be a relative key prefix without %q", req.Prefix, ".."))
	}
	c, err := s.client()
	if err != nil {
		return storage.ListPage{}, err
	}
	in := &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.Bucket), Prefix: aws.String(s.key(req.Prefix))}
	if req.Token != "" {
		in.ContinuationToken = aws.String(req.Token)
	}
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	out, err := c.ListObjectsV2(rctx, in)
	if err != nil {
		return storage.ListPage{}, s.classify(ctx, err, "list "+aws.ToString(in.Prefix))
	}
	page := storage.ListPage{}
	if aws.ToBool(out.IsTruncated) {
		page.NextToken = aws.ToString(out.NextContinuationToken)
	}
	base := s.key("")
	for _, o := range out.Contents {
		k := aws.ToString(o.Key)
		if !strings.HasPrefix(k, base) {
			continue // not this storage's key: the prefix lookup never returns it
		}
		page.Objects = append(page.Objects, model.ObjectInfo{Key: strings.TrimPrefix(k, base), Size: aws.ToInt64(o.Size),
			ModTime: aws.ToTime(o.LastModified), MD5: etagMD5(o.ETag)})
	}
	return page, nil
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := s.client()
	if err != nil {
		return err
	}
	full := s.key(key)
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	if _, err := c.DeleteObject(rctx, &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(full)}); err != nil {
		return s.classify(ctx, err, "delete "+full)
	}
	return nil
}

func (s *Storage) Get(ctx context.Context, key string, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := s.client()
	if err != nil {
		return err
	}
	full := s.key(key)
	// request_timeout bounds the wait for the response headers only: the body is
	// a stream and belongs to the caller's deadline, however slow the destination
	// is. A timer that could not be stopped has cancelled the request already, so
	// whatever answer still arrived is not one to read.
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(s.cfg.RequestTimeout.Std(), cancel)
	out, err := c.GetObject(rctx, &s3.GetObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(full)})
	timedOut := !timer.Stop()
	if out != nil && out.Body != nil {
		defer func() { _ = out.Body.Close() }()
	}
	if timedOut {
		return pluginerrors.Transient(fmt.Errorf("get %s: no response within request_timeout %s", full, s.cfg.RequestTimeout))
	}
	if err != nil {
		if isNotFound(err) {
			return pluginerrors.NotFound(fmt.Errorf("object %s is definitely absent", full))
		}
		return s.classify(ctx, err, "get "+full)
	}
	n, err := io.Copy(dst, out.Body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return pluginerrors.Transient(fmt.Errorf("read %s: %w", full, err))
	}
	if out.ContentLength != nil && n != *out.ContentLength {
		return pluginerrors.Transient(fmt.Errorf("download of %s stopped at %d of %d bytes", full, n, *out.ContentLength))
	}
	return nil
}

// key builds an object's full key inside the bucket from a storage-relative
// one; key("") is the storage root, trailing slash included, under which
// listed keys are reported.
func (s *Storage) key(rel string) string {
	p := s.cfg.Prefix
	if p != "" {
		p += "/"
	}
	return p + rel
}

// classify maps an SDK error to a plugin error class. The HTTP status decides,
// with the service's own retry-after hint passed through for the transient
// ones; 404 is "definitely absent" only for callers that say so — an existence
// probe answered 403 must never be read as absence. A request that got no HTTP
// answer at all is a connection or timeout problem: transient, unless the
// caller's own context ended, which stays unclassified and terminal.
func (s *Storage) classify(ctx context.Context, err error, what string) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	wrapped := fmt.Errorf("%s: %w", what, err)
	switch apiErrorCode(err) {
	case "NoSuchBucket":
		return pluginerrors.Permanent(wrapped)
	case "RequestTimeout":
		return pluginerrors.Transient(wrapped)
	}
	switch status := apiStatus(err); {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return pluginerrors.Permanent(wrapped)
	case status == http.StatusTooManyRequests || status >= 500:
		if d := retryAfterHint(err); d > 0 {
			return pluginerrors.RetryAfter(wrapped, d)
		}
		return pluginerrors.Transient(wrapped)
	case status == http.StatusNotFound:
		return pluginerrors.NotFound(wrapped)
	case status >= 400:
		return pluginerrors.Permanent(wrapped)
	default:
		return pluginerrors.Transient(wrapped)
	}
}

// isNotFound reports the service's own definite-absence answer: a 404 that is
// not the bucket itself missing.
func isNotFound(err error) bool {
	return apiStatus(err) == http.StatusNotFound && apiErrorCode(err) != "NoSuchBucket"
}

func apiErrorCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return ""
}

func apiStatus(err error) int {
	// *smithyhttp.ResponseError, which every SDK API error embeds, answers
	// HTTPStatusCode, so this one assertion covers the whole error tree.
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		return httpErr.HTTPStatusCode()
	}
	return 0
}

func retryAfterHint(err error) time.Duration {
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) || respErr.Response == nil {
		return 0
	}
	v := respErr.Response.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := time.ParseDuration(v + "s"); err == nil && secs >= 0 {
		return secs
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// etagMD5 returns the ETag as an md5 hex when the service's ETag is the object's
// plain md5 — what a single-part PutObject returns. A multipart ETag
// ("<md5-of-md5s>-<n>") is not the object's md5 and is reported as no checksum.
func etagMD5(etag *string) string {
	s := strings.Trim(aws.ToString(etag), `"`)
	if len(s) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(s); err != nil {
		return ""
	}
	return strings.ToLower(s)
}

// readSecret reads one mounted secret file. The value is stored trimmed: keys
// and tokens never carry surrounding whitespace, and a trailing newline from
// however the file was produced would silently break every signature.
func readSecret(sec configuration.Secret) (string, error) {
	b, err := os.ReadFile(sec.File)
	if err != nil {
		return "", pluginerrors.Permanent(fmt.Errorf("read secret file: %w", err))
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", pluginerrors.Permanent(fmt.Errorf("secret file %s is empty", sec.File))
	}
	return v, nil
}
