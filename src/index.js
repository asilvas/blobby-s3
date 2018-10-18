import knox from 'knox';
import path from 'path';
import farmhash from 'farmhash';
import extend from 'extend';
import async from 'async';
import once from 'once';
import http from 'http';
import https from 'https';
import crypto from 'crypto';

export default class BlobbyS3 {
  constructor(opts) {
    this.options = extend({
      endpoint: 's3.amazonaws.com',
      port: 80,
      secure: false,
      style: 'path',
      agent: undefined // use http.globalAgent by default
    }, opts);

    if (typeof this.options.agent === 'object') {
      this.agent = new http.Agent(this.options.agent);
    } else {
      this.agent = this.options.agent; // otherwise forward value (be it false or undefined)
    }
    delete this.options.agent; // remove from options to avoid needless merging
  }

  initialize(cb) {
    const { bucketPrefix, bucketStart, bucketEnd, lifecycle } = this.options;

    const initBucketTasks = [];

    const $this = this;
    const getInitBucketTask = bucketIndex => {
      return cb => {
        cb = once(cb);
        const client = $this.getClient('', { forcedIndex: bucketIndex !== undefined ? bucketIndex : null });

        const req = client.request('PUT', '');
        req.on('response', res => {
          res.resume();
          cb();

          if (lifecycle && lifecycle.days) {
            const body = `<LifecycleConfiguration><Rule><ID>TTL</ID><Filter><Prefix>${lifecycle.prefix || ''}</Prefix></Filter><Status>Enabled</Status><Expiration><Days>${lifecycle.days}</Days></Expiration></Rule></LifecycleConfiguration>`;
            const bodyMD5 = crypto.createHash('md5').update(body).digest('base64');
            const req2 = client.request('PUT', '/?lifecycle', {
              'Content-MD5': bodyMD5,
              'Content-Length': body.length
            });
            req2.on('response', res => {
              if (res.statusCode <= 204) {
                console.log('LifeCycle rules applied successfully');
                return void res.resume();
              }

              console.error('LifeCycle request:', body);
              console.error('LifeCycle rules failed with status:', res.statusCode);

              res.setEncoding('utf8');
              res.on('data', data => console.error('LifeCycle response:', data));
            });
            req2.write(body);
            req2.end();
          }
        });
        req.on('error', cb);
        req.end();
      }
    };

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
    async.parallelLimit(initBucketTasks, 10, cb);
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
    cb = once(cb);

    if (opts.acl === 'public') {
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
    client.headFile(fileKey, function (err, res) {
      if (err) {
        return void cb(err);
      }

      res.resume(); // discard response

      if (res.statusCode !== 200) {
        return void cb(new Error('storage.s3.fetch.error: '
          + res.statusCode + ' for ' + (client.urlBase + '/' + fileKey))
        );
      }

      cb(null, getInfoHeaders(res.headers));
    });
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
    cb = once(cb);
    const dir = path.dirname(fileKey);

    if (opts.acl === 'public') {
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
    const bufs = [];
    client.getFile(fileKey, function (err, res) {
      if (err) {
        return void cb(err);
      }

      res.on('data', function (chunk) {
        bufs.push(chunk);
      });

      res.on('end', function () {
        if (res.statusCode !== 200) {
          return void cb(new Error('storage.s3.fetch.error: '
            + res.statusCode + ' for ' + (client.urlBase + '/' + fileKey))
          );
        }

        cb(null, getInfoHeaders(res.headers), Buffer.concat(bufs));
      });
    });
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

    cb = once(cb);
    client.putBuffer(file.buffer, fileKey, getHeadersFromInfo(file.headers), function (err, res) {
      if (err) {
        return void cb(err);
      }

      res.resume(); // discard response

      if (res.statusCode !== 200) {
        return void cb(new Error('storage.s3.store.error: '
          + res.statusCode + ' for ' + (client.urlBase + '/' + fileKey))
        );
      }

      cb(null, getInfoHeaders(res.headers));
    });
  }

  // support for x-amz-copy-source: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
  copy(sourceKey, destKey, options /* optional */, cb) {
    if (typeof options === 'function') {
      cb = options;
      options = {}; // optional
    }

    const client = this.getClient(path.dirname(sourceKey), { bucket: options.bucket });
    const destBucket = this.getShard(path.dirname(destKey));

    cb = once(cb);
    const destHeaders = {
      'x-amz-acl': options.AccessControl || 'public-read'
    };
    const req = client.copyTo(sourceKey, destBucket, destKey, destHeaders);
    req.on('response', res => {
      if (res.statusCode !== 200) {
        return void cb(new Error('storage.s3.copy.error: '
          + res.statusCode + ' for ' + (client.urlBase + '/' + destKey))
        );
      }

      res.resume(); // discard response
      
      cb();
    });
    req.on('error', cb);
    req.end();
  }

  setACL(fileKey, acl, cb) {
    const client = this.getClient(path.dirname(fileKey));

    // NOTICE: unfortunately there is no known way of forcing S3 to persist the SOURCE ETag or LastModified, so comparing
    // between foreign sources (S3 and FS) S3 will always report the file as different...

    cb = once(cb);

    const req = client.request('PUT', encodeSpecialCharacters(fileKey) + '?acl', { 'x-amz-acl': acl });
    req.on('response', res => {
      if (res.statusCode !== 200) {
        return void cb(new Error('storage.s3.setACL.error: '
          + res.statusCode + ' for ' + (client.urlBase + '/' + fileKey))
        );
      }

      res.resume(); // discard response
      
      cb();
    });
    req.on('error', cb);
    req.end();
  }

  /*
   fileKey: unique id for storage
   */
  remove(fileKey, cb) {
    const client = this.getClient(path.dirname(fileKey));

    cb = once(cb);
    client.deleteFile(fileKey, function (err, res) {
      if (err) {
        return void cb(err);
      }

      res.resume(); // discard response

      if (res.statusCode !== 200 && res.statusCode !== 204) {
        return void cb(new Error(`S3.remove executed 2xx for ${fileKey} but got ${res.statusCode}`));
      }

      cb();
    });
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
        client.deleteMultiple(files.map(f => f.Key), (err, res) => {
          if (err) return void cb(err);

          res.resume(); // discard response

          if (res.statusCode !== 200) {
            return void cb(new Error('storage.s3.removeDirectory.error: '
              + res.statusCode + ' for ' + (client.urlBase + '/' + dir))
            );
          }

          filesDeleted += files.length;
          if (!lastKey) return cb(null, filesDeleted); // no more to delete 

          // continue recursive deletions
          _listAndDelete(dir, lastKey, cb);
        });
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
      prefix: dir + ((dir.length === 0 || dir[dir.length - 1] === '/') ? '' : '/'), // prefix must always end with `/` if not root
      delimiter: typeof opts.delimiter === 'string' ? opts.delimiter : opts.deepQuery ? '' : '/'
    };
    let forcedBucketIndex;
    const { bucketStart, bucketEnd } = this.options;
    if (opts.lastKey) {
      const lastKeySplit = opts.lastKey.split(':');
      forcedBucketIndex = parseInt(lastKeySplit[0]);
      if (lastKeySplit.length > 1) { // only set a (resume) marker if one was provided
        params.marker = lastKeySplit.slice(1).join(':'); // rebuild marker after removing index
      }
    } else if (opts.deepQuery && typeof bucketStart === 'number' && typeof bucketEnd === 'number') {
      // if no key provided, doing a deep query, default forcedBucketIndex
      forcedBucketIndex = bucketStart;
    }
    if (opts.maxKeys) params['max-keys'] = opts.maxKeys;

    const client = this.getClient(dir, { forcedIndex: forcedBucketIndex });
    cb = once(cb);
    client.list(params, (err, data) => {
      data = data || {}; // default in case of error
      if (err) return void cb(err);
      // only return error if not related to key not found (commonly due to bucket deletion)
      if (data.Code && data.Code !== 'NoSuchKey') return cb(new Error(data.Code));

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
    });
  }

  /*
   This is not a persisted client, so it's OK to create
   one instance per request.
  */
  getClient(dir, { forcedIndex, bucket } = {}) {
    bucket = bucket || this.getShard(dir, forcedIndex);

    const opts = extend(true, { }, this.options, { bucket });
    opts.agent = this.agent;
    return knox.createClient(opts);
  }

  getShard(dir, forcedIndex) {
    const {bucketPrefix, bucketStart, bucketEnd} = this.options;
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

  httpRequest(method, bucket, fileKey, cb) {
    var client = this.options.secure ? https : http;

    const opts = {
      protocol: this.options.secure ? 'https:' : 'http:',
      host: this.options.style === 'path' ? this.options.endpoint
        : `${bucket}.${this.options.endpoint}`, // fallback to subdomain
      port: this.options.port,
      agent: this.agent, // use same agent as knox
      method,
      path: encodeSpecialCharacters(this.options.style === 'path' ? `/${bucket}/${fileKey}`
        : `/${fileKey}`)
    };

    var bufs = [];
    client.request(opts, res => {
      if (res.statusCode !== 200) {
        return void cb(new Error('http.request.error: '
          + res.statusCode + ' for ' + opts.path)
        );
      }

      res.on('data', chunk => bufs.push(chunk));

      res.on('end', () => cb(null, res, Buffer.concat(bufs)));
    }).on('error', err => {
      cb(err);
    }).end();
  }
}

const gValidHeaders = {
  'last-modified': 'LastModified',
  'content-length': 'Size',
  'etag': 'ETag',
  'content-type': 'ContentType'
};
const gReverseHeaders = {
  ContentType: 'Content-Type',
  AccessControl: 'x-amz-acl'
};

function getInfoHeaders(reqHeaders) {
  const info = {};
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
    info[validHeader] = validHeader === 'LastModified' ? new Date(val) : val; // map the values
    if (validHeader === 'Size') info[validHeader] = parseInt(val); // number required for Size
  });

  return info;
}

function getHeadersFromInfo(info) {
  const headers = {};
  Object.keys(info).forEach(k => {
    const validHeader = gReverseHeaders[k];
    if (!validHeader) return;
    const val = info[k];
    if (!val) return;
    headers[validHeader] = val; // map the values
  });
  Object.keys(info.CustomHeaders || {}).forEach(k => {
    const validHeader = /^amz\-meta\-/i.test(k);
    if (!validHeader) return;
    headers['x-' + k] = info.CustomHeaders[k];
  });

  return headers;
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
