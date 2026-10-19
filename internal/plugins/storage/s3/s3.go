// Package s3 is the storage plugin over any S3-compatible endpoint.
//
// One attempt per call: the SDK's own retryer is disabled (aws.NopRetryer) and
// every error is classified, so the engine owns retries exactly as it does for
// every other storage. Put stores one object in ONE PutObject request; an object
// larger than a part travels part by part through the storage.MultipartUploader
// capability, which the engine drives (see multipart.go) — one part per call and
// no buffer of the object held here, so a part that failed is sent again without
// re-sending the parts the service already accepted.
//
// A write is successful only once HeadObject reports the committed size, so a
// 200 that did not commit anything (CompleteMultipartUpload may answer 200 with
// an error in the body) is a failed attempt, not a stored object.
package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
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

type Config struct {
	Endpoint        string         `json:"endpoint" required:"true" doc:"base URL of the S3 endpoint, e.g. https://s3.example.com"`
	Region          string         `json:"region" required:"true" doc:"signing region of the endpoint"`
	Bucket          string         `json:"bucket" required:"true" doc:"bucket every object lives in"`
	Prefix          string         `json:"prefix" required:"true" doc:"key prefix of the storage root; keys are built under it"`
	AccessKeyID     plugin.Secret  `json:"access_key_id" doc:"file holding the access key id; an opaque string, e.g. <tenant_id>:<key_id>"`
	SecretAccessKey plugin.Secret  `json:"secret_access_key" doc:"file holding the secret access key"`
	Addressing      string         `json:"addressing" required:"true" doc:"addressing style: path or virtual"`
	StorageClass    string         `json:"storage_class" doc:"storage class of every object, e.g. STANDARD"`
	RequestChecksum string         `json:"request_checksum" doc:"request checksum mode: when_required or when_supported"`
	PartBytes       model.ByteSize `json:"part_bytes" doc:"bytes per part of a multipart upload; at least 5 MiB"`
	MaxRequestBytes model.ByteSize `json:"max_request_bytes" doc:"largest single request the endpoint accepts"`
	MaxObjectBytes  model.ByteSize `json:"max_object_bytes" doc:"largest object the endpoint accepts"`
	RequestTimeout  model.Duration `json:"request_timeout" doc:"timeout of one HTTP request"`
}

// Register adds the S3 storage to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, storage.Kind, plugin.Spec[Config, storage.Storage]{
		Type:      "s3",
		Summary:   "an S3-compatible endpoint",
		Stability: plugin.Stable,
		Example: []byte(`{"endpoint": "https://s3.example.com", "region": "ru-central-1", "bucket": "bucket-name",` +
			` "prefix": "cluster-backup", "access_key_id": {"file": "/run/secrets/s3/access-key-id"},` +
			` "secret_access_key": {"file": "/run/secrets/s3/secret-access-key"}, "addressing": "path",` +
			` "storage_class": "STANDARD", "request_checksum": "when_required", "part_bytes": 16777216,` +
			` "max_request_bytes": 5368709120, "max_object_bytes": 5497558138880,` +
			` "request_timeout": "5m"}`),
		New: func(ctx context.Context, cfg Config, deps plugin.Deps) (storage.Storage, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return newStorage(cfg)
		},
	})
}

func newStorage(cfg Config) (*Storage, error) {
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
	if cfg.StorageClass == "" {
		return nil, errors.New("storage_class is empty")
	}
	known := false
	for _, sc := range types.StorageClass("").Values() {
		if string(sc) == cfg.StorageClass {
			known = true
			break
		}
	}
	if !known {
		return nil, fmt.Errorf("storage_class %q is not one S3 defines", cfg.StorageClass)
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
	return &Storage{cfg: cfg, checksum: checksum}, nil
}

type Storage struct {
	cfg      Config
	checksum aws.RequestChecksumCalculation
	// http is the client every request goes through; nil is the SDK's own
	// default. A test substitutes one to hand a request an answer whose body is
	// shorter than the length it declares — the shape Get's own length guard
	// exists for, and one no real transport produces.
	http aws.HTTPClient
}

func (s *Storage) Describe(ctx context.Context) (storage.StorageTraits, error) {
	if err := ctx.Err(); err != nil {
		return storage.StorageTraits{}, err
	}
	return storage.StorageTraits{Checksums: []string{"md5"}}, nil
}

var _ plugin.Checker = (*Storage)(nil)

// Check is the instance's preflight: one read-only listing probe at the
// configured bucket and prefix proves the credentials are accepted and the
// bucket is reachable. MaxKeys keeps the probe to at most one key, and an empty
// prefix is not an error — the first run writes into it. A bucket that does not
// exist is a permanent error that names it, as is a rejected credential. One
// attempt: the engine retries a transient answer under the configured policy.
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
			return plugin.Permanent(fmt.Errorf("check bucket %s: the credentials were rejected: %w", s.cfg.Bucket, err))
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
		if s.http != nil { // nil leaves the SDK's own client in place
			o.HTTPClient = s.http
		}
	}), nil
}

func (s *Storage) reqCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.cfg.RequestTimeout.Std())
}

// Put stores one object in one PutObject request and returns only after
// HeadObject confirms the committed size. An object larger than one request is a
// permanent error here: the engine uploads such an object part by part through
// the storage.MultipartUploader capability, and Put never becomes a multipart
// upload of its own.
func (s *Storage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	if req.Size > s.cfg.MaxObjectBytes.Int64() {
		return model.ObjectInfo{}, plugin.Permanent(
			fmt.Errorf("object %s is %d bytes, above the endpoint limit of %d", req.Key, req.Size, s.cfg.MaxObjectBytes))
	}
	if req.Size > s.cfg.MaxRequestBytes.Int64() {
		return model.ObjectInfo{}, plugin.Permanent(fmt.Errorf(
			"object %s is %d bytes, above the %d bytes one request carries: it is uploaded part by part through the multipart capability",
			req.Key, req.Size, s.cfg.MaxRequestBytes))
	}
	c, err := s.client()
	if err != nil {
		return model.ObjectInfo{}, err
	}
	full := s.key(req.Key)
	if err := s.putSingle(ctx, c, full, req.Size, body); err != nil {
		return model.ObjectInfo{}, err
	}
	return s.verify(ctx, c, req.Key, full, req.Size)
}

// putSingle sends the whole object in one request — the only path that can be
// checked against the request's own md5 later, because a single-part PutObject
// returns the object's md5 as its ETag. A body shorter than ContentLength fails
// the request inside net/http, so the declared size is never a claim the send
// did not honour.
func (s *Storage) putSingle(ctx context.Context, c *s3.Client, full string, size int64, body io.Reader) error {
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	_, err := c.PutObject(rctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.cfg.Bucket),
		Key:           aws.String(full),
		Body:          body,
		ContentLength: aws.Int64(size),
		StorageClass:  types.StorageClass(s.cfg.StorageClass),
	})
	if err != nil {
		return s.classify(ctx, err, "put "+full)
	}
	return nil
}

// verify is the only proof a write landed: HeadObject must report the sent
// size. A missing object right after a successful upload is a failed commit,
// not the not_found of an absent key, so it stays transient here.
func (s *Storage) verify(ctx context.Context, c *s3.Client, key, full string, size int64) (model.ObjectInfo, error) {
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	out, err := c.HeadObject(rctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(full)})
	if err != nil {
		if isNotFound(err) {
			return model.ObjectInfo{}, plugin.Transient(
				fmt.Errorf("object %s is not visible after the upload (not committed)", full))
		}
		return model.ObjectInfo{}, s.classify(ctx, err, "head "+full)
	}
	if aws.ToInt64(out.ContentLength) != size {
		return model.ObjectInfo{}, plugin.Transient(
			fmt.Errorf("size of %s after upload is %d, want %d: the upload is not committed",
				full, aws.ToInt64(out.ContentLength), size))
	}
	return model.ObjectInfo{Key: key, Size: aws.ToInt64(out.ContentLength), ModTime: aws.ToTime(out.LastModified),
		MD5: etagMD5(out.ETag)}, nil
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
			return model.ObjectInfo{}, plugin.NotFound(fmt.Errorf("object %s is definitely absent", full))
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
		return storage.ListPage{}, plugin.Permanent(
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
	rctx, cancel := s.reqCtx(ctx)
	defer cancel()
	out, err := c.GetObject(rctx, &s3.GetObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(full)})
	if err != nil {
		if isNotFound(err) {
			return plugin.NotFound(fmt.Errorf("object %s is definitely absent", full))
		}
		return s.classify(ctx, err, "get "+full)
	}
	defer func() { _ = out.Body.Close() }()
	n, err := io.Copy(dst, out.Body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return plugin.Transient(fmt.Errorf("read %s: %w", full, err))
	}
	if out.ContentLength != nil && n != *out.ContentLength {
		return plugin.Transient(fmt.Errorf("download of %s stopped at %d of %d bytes", full, n, *out.ContentLength))
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
		return plugin.Permanent(wrapped)
	case "RequestTimeout":
		return plugin.Transient(wrapped)
	}
	switch status := apiStatus(err); {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return plugin.Permanent(wrapped)
	case status == http.StatusTooManyRequests || status >= 500:
		if d := retryAfterHint(err); d > 0 {
			return plugin.RetryAfter(wrapped, d)
		}
		return plugin.Transient(wrapped)
	case status == http.StatusNotFound:
		return plugin.NotFound(wrapped)
	case status >= 400:
		return plugin.Permanent(wrapped)
	default:
		return plugin.Transient(wrapped)
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
func readSecret(sec plugin.Secret) (string, error) {
	b, err := os.ReadFile(sec.File)
	if err != nil {
		return "", plugin.Permanent(fmt.Errorf("read secret file: %w", err))
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", plugin.Permanent(fmt.Errorf("secret file %s is empty", sec.File))
	}
	return v, nil
}
