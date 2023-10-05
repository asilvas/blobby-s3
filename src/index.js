const path = require('path');
const farmhash = require('farmhash');
const once = require('once');
const http = require('http');
const https = require('https');
const URL = require('url');
const { S3 } = require('@aws-sdk/client-s3');
const plimit = require('p-limit');
const HttpError = require('./httpError');

module.exports = class BlobbyS3 {
  constructor(opts) {
    this.options = Object.assign({
      privateOnly: true
    }, opts);

    // some backward compatibility to ease transition from knox
    if (typeof this.options.style === 'string') {
      this.options.s3ForcePathStyle = this.options.style === 'path';
      delete this.options.style;
    }
    if (typeof this.options.key === 'string') {
      this.options.accessKeyId = this.options.key;
      delete this.options.key;
    }
    if (typeof this.options.secret === 'string') {
      this.options.secretAccessKey = this.options.secret;
      delete this.options.secret;
    }
    if (this.options.agent) {
      this.options.httpOptions = { agent: this.options.agent };
      delete this.options.agent;
    }
    if (this.options.httpOptions && typeof this.options.httpOptions.agent === 'object') {
      this.httpOptions.agent = new http.Agent(this.options.httpOptions.agent);
    }

    this.endpoint = this.options.endpoint && URL.parse(this.options.endpoint);
    this.agent = this.options.httpOptions && this.options.httpOptions.agent;
    this.http = this.endpoint && this.endpoint.protocol === 'http:' ? http : https;
  }

  /*
   This is not a persisted client, so it's OK to create
   one instance per request.
  */
  getClient(dir, { forcedIndex, bucket } = {}) {
    bucket = bucket || this.getShard(dir, forcedIndex);

    // copy
    const { bucketPrefix, bucketStart, bucketEnd, privateOnly, ...opts } = this.options;

    const s3 = new S3(opts);
    s3.bucket = bucket;

    return s3;
  }

  getShard(dir, forcedIndex) {
    const { bucketPrefix, bucketStart, bucketEnd } = this.options;
    let bucket = bucketPrefix;
    const range = bucketEnd - bucketStart;
    if (!isNaN(range) && range > 0) {
      if (typeof forcedIndex === 'number' && forcedIndex >= bucketStart && forcedIndex <= bucketEnd) {
        // if forced index is provided, use that instead
        bucket += forcedIndex;
      } else { // compute bucket by dir hash
        // hash only needs to span directory structure, and is OK to be consistent
        // across environments for predictability and ease of maintenance
        const hash = farmhash.hash32(dir);
        const bucketIndex = hash % (range + 1); // 0-N
        bucket += (bucketStart + bucketIndex);
      }
    }

    return bucket;
  }

  initialize(cb) {
    const { bucketPrefix, bucketStart, bucketEnd, lifecycle } = this.options;

    if (!lifecycle || !lifecycle.days) return void cb();

    const initBucketTasks = [];

    const limit = plimit(10);

    const $this = this;
    const getInitBucketTask = bucketIndex => limit(() => {
      const client = $this.getClient('', { forcedIndex: bucketIndex !== undefined ? bucketIndex : null });

      const params = {
        Bucket: client.bucket,
        LifecycleConfiguration: {
          Rules: [
            {
              ID: 'TTL',
              Filter: {
                Prefix: lifecycle.prefix || ''
              },
              Status: "Enabled",
              Expiration: {
                Days: lifecycle.days
              }
            }
          ]
        }
      };

      return client.putBucketLifecycleConfiguration(params)
        .then(() => console.log('LifeCycle rules applied successfully'))
        .catch(err => console.error('Failed to apply lifecycle rules:', params, err.stack || err))
        ;

    });

    const range = bucketEnd - bucketStart;
    if (!isNaN(range) && range > 0) {
      // sharded bucket mode
      for (let i = bucketStart; i <= bucketEnd; i++) {
        initBucketTasks.push(getInitBucketTask(i));
      }
    } else {
      // single bucket mode
      initBucketTasks.push(getInitBucketTask(bucketPrefix));
    }

    Promise.all(initBucketTasks).then(cb).catch(cb);
  }

  /*
    fileKey: unique id for storage
    opts: future
   */
  fetchInfo(fileKey, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};
    const dir = path.dirname(fileKey);

    if (!this.options.privateOnly && opts.acl === 'public') {
      const bucket = this.getShard(dir);

      return void this.httpRequest('HEAD', bucket, fileKey, (err, res, data) => {
        if (err) {
          return void cb(err);
        }

        cb(null, getInfoHeaders(res.headers));
      });
    }
    // else assume private

    const client = this.getClient(dir);
    const params = {
      Bucket: client.bucket,
      Key: fileKey
    };
    client.headObject(params).then(data => cb(null, getInfoHeaders(data)), err => cb(err));
  }

  /*
    fileKey: unique id for storage
    opts: future
   */
  fetch(fileKey, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};
    const dir = path.dirname(fileKey);
    if (!this.options.privateOnly && opts.acl === 'public') {
      const bucket = this.getShard(dir);

      return void this.httpRequest('GET', bucket, fileKey, (err, res, data) => {
        if (err) {
          return void cb(err);
        }

        cb(null, getInfoHeaders(res.headers), data);
      });
    }
    // else assume private

    const client = this.getClient(dir);
    const params = {
      Bucket: client.bucket,
      Key: fileKey
    };
    client.getObject(params).then(res => res.Body.transformToByteArray()).then(data => cb(null, getInfoHeaders(data), data.Body), cb);
  }

  /*
   fileKey: unique id for storage
   file: file object
   file.buffer: Buffer containing file data
   file.headers: Any HTTP headers to supply to object
   opts: future
   */
  store(fileKey, file, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};
    const client = this.getClient(path.dirname(fileKey));

    // NOTICE: unfortunately there is no known way of forcing S3 to persist the SOURCE ETag or LastModified, so comparing
    // between foreign sources (S3 and FS) S3 will always report the file as different...
    const params = {
      Body: file.buffer,
      Key: fileKey,
      Bucket: client.bucket,
      ...getHeadersFromInfo(file.headers, this.options)
    };

    client.putObject(params).then(data => cb(null, getInfoHeaders(data)), cb);
  }

  // support for x-amz-copy-source: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
  copy(sourceKey, destKey, options /* optional */, cb) {
    if (typeof options === 'function') {
      cb = options;
      options = {}; // optional
    }

    const client = this.getClient(path.dirname(sourceKey), { bucket: options.bucket });
    const destBucket = this.getShard(path.dirname(destKey));

    const params = {
      Bucket: destBucket,
      CopySource: `/${client.bucket}/${sourceKey}`,
      Key: destKey,
      ACL: (!this.options.privateOnly ? options.AccessControl : 'private') || 'public-read',
      ContentType: options.ContentType,
      MetadataDirective: options.CopyAndReplace ? 'REPLACE' : 'COPY'
    };

    client.copyObject(params).then(data => cb(null, getInfoHeaders(data)), cb);
  }

  setACL(fileKey, acl, cb) {
    const client = this.getClient(path.dirname(fileKey));

    // NOTICE: unfortunately there is no known way of forcing S3 to persist the SOURCE ETag or LastModified, so comparing
    // between foreign sources (S3 and FS) S3 will always report the file as different...

    const params = {
      Bucket: client.bucket,
      Key: fileKey,
      ACL: acl
    };

    client.putObjectAcl(params).then(cb, cb);
  }

  /*
   fileKey: unique id for storage
   */
  remove(fileKey, cb) {
    const client = this.getClient(path.dirname(fileKey));

    const params = {
      Bucket: client.bucket,
      Key: fileKey
    };

    client.deleteObject(params).then(cb, cb);
  }

  /*
   dir: unique id for storage
   */
  removeDirectory(dir, cb) {
    const $this = this;
    let filesDeleted = 0;
    const _listAndDelete = (dir, lastKey, cb) => {
      $this.list(dir, { maxKeys: 1000, delimiter: '', lastKey }, (err, files, dirs, lastKey) => {
        if (err) return void cb(err);

        if (files.length === 0) return void cb(null, filesDeleted);

        const client = $this.getClient(dir);
        const params = {
          Bucket: client.bucket,
          Delete: {
            Objects: files.map(f => ({ Key: f.Key })),
            Quiet: false
          }
        };
        client.deleteObjects(params).then(() => {
          filesDeleted += files.length;
          if (!lastKey) return cb(null, filesDeleted); // no more to delete

          // continue recursive deletions
          _listAndDelete(dir, lastKey, cb);
        }, cb);
      });
    };

    _listAndDelete(dir, null, cb);
  }

  /* supported options:
   dir: Directory (prefix) to query
   opts: Options object
   opts.lastKey: if requesting beyond maxKeys (paging)
   opts.maxKeys: the max keys to return in one request
   opts.delimiter: can be used to control delimiter of query independent of deepQuery
   opts.deepQuery: true if you wish to query the world across buckets, not just the current directory
   cb(err, files, dirs, lastKey) - Callback fn
   cb.err: Error if any
   cb.files: An array of files: { Key, LastModified, ETag, Size, ... }
   cb.dirs: An array of dirs: [ 'a', 'b', 'c' ]
   cb.lastKey: An identifier to permit retrieval of next page of results, ala: 'abc'
  */
  list(dir, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};

    const params = {
      Prefix: dir + ((dir.length === 0 || dir[dir.length - 1] === '/') ? '' : '/'), // prefix must always end with `/` if not root
      Delimiter: typeof opts.delimiter === 'string' ? opts.delimiter : opts.deepQuery ? '' : '/'
    };
    let forcedBucketIndex;
    const { bucketStart, bucketEnd } = this.options;
    if (opts.lastKey) {
      const lastKeySplit = opts.lastKey.split(':');
      forcedBucketIndex = parseInt(lastKeySplit[0]);
      if (lastKeySplit.length > 1) { // only set a (resume) marker if one was provided
        params.Marker = lastKeySplit.slice(1).join(':'); // rebuild marker after removing index
      }
    } else if (opts.deepQuery && typeof bucketStart === 'number' && typeof bucketEnd === 'number') {
      // if no key provided, doing a deep query, default forcedBucketIndex
      forcedBucketIndex = bucketStart;
    }
    if (opts.maxKeys) params.MaxKeys = opts.maxKeys;

    const client = this.getClient(dir, { forcedIndex: forcedBucketIndex });
    params.Bucket = client.bucket;
    client.listObjects(params).then(data => {
      const files = data.Contents || [];
      // remove S3's tail delimiter
      const dirs = (data.CommonPrefixes || []).map(dir => dir.Prefix.substr(0, dir.Prefix.length - 1));

      let lastKey = data.IsTruncated ? (data.NextMarker || data.Contents[data.Contents.length - 1].Key) : null;
      if (!lastKey && typeof forcedBucketIndex === 'number' && forcedBucketIndex < bucketEnd) {
        // if out of keys, but not out of buckets, increment to next bucket
        lastKey = `${forcedBucketIndex + 1}:`;
      } else if (lastKey) {
        // prefix with bucket
        lastKey = `${typeof forcedBucketIndex === 'number' ? forcedBucketIndex : ''}:${lastKey}`;
      }

      cb(null, files, dirs, lastKey);
    }, cb);
  }

  httpRequest(method, bucket, fileKey, cb) {
    const opts = {
      protocol: this.endpoint.protocol || 'https:',
      host: this.options.s3ForcePathStyle ? this.endpoint.host
        : `${bucket}.${this.endpoint.host}`, // fallback to subdomain
      port: this.endpoint.port,
      agent: this.agent, // use same agent as s3 client
      method,
      path: encodeSpecialCharacters(this.options.s3ForcePathStyle ? `/${bucket}/${fileKey}`
        : `/${fileKey}`)
    };

    cb = once(cb);

    var bufs = [];
    this.http.request(opts, res => {
      const { statusCode } = res;
      if (statusCode !== 200) {
        const message = `http.request.error: ${statusCode} for ${opts.path}`;
        return void cb(new HttpError(message, statusCode));
      }

      res.on('data', chunk => bufs.push(chunk));

      res.on('end', () => cb(null, res, Buffer.concat(bufs)));
    }).on('error', err => {
      cb(err);
    }).end();
  }
}

const gValidHeaders = {
  'lastmodified': 'LastModified',
  'last-modified': 'LastModified',
  'cache-control': 'CacheControl',
  'cachecontrol': 'CacheControl',
  'contentlength': 'Size',
  'content-length': 'Size',
  'etag': 'ETag',
  'content-type': 'ContentType',
  'contenttype': 'ContentType'
};

function getInfoHeaders(reqHeaders) {
  const info = {};
  if (reqHeaders.Metadata) {
    info.CustomHeaders = reqHeaders.Metadata;
  }
  Object.keys(reqHeaders).forEach(k => {
    const kLower = k.toLowerCase();
    const validHeader = gValidHeaders[kLower];
    if (!validHeader) {
      if (/^x-amz-meta-/.test(kLower)) {
        info.CustomHeaders = info.CustomHeaders || {};
        info.CustomHeaders[kLower.substr(2)] = reqHeaders[k];
      }
      return;
    }
    const val = reqHeaders[k];
    if (!val) return;
    info[validHeader] = validHeader === 'LastModified' && typeof val === 'string' ? new Date(val) : val; // map the values
    if (validHeader === 'Size') info[validHeader] = parseInt(val, 10); // number required for Size
  });

  return info;
}

function getHeadersFromInfo(info, options) {
  const ret = {};

  if (info.ContentType) {
    ret.ContentType = info.ContentType;
  }

  if (info.CacheControl) {
    ret.CacheControl = info.CacheControl;
  }

  if (info.AccessControl) {
    ret.ACL = !options.privateOnly ? info.AccessControl : 'private';
  }

  if (info.CustomHeaders) {
    ret.Metadata = info.CustomHeaders;
  }

  return ret;
}

/* PULLED FROM knox
  https://github.com/Automattic/knox/blob/master/lib/client.js#L64-L70
*/
function encodeSpecialCharacters(filename) {
  // Note: these characters are valid in URIs, but S3 does not like them for
  // some reason.
  return encodeURI(filename).replace(/[!'()#*+? ]/g, function (char) {
    return '%' + char.charCodeAt(0).toString(16);
  });
}
