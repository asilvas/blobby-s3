# blobby-s3

An S3 client for [Blobby](https://github.com/asilvas/blobby), powered
by [AWS S3](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#constructor-property).



## Options

```
# config/local.json5
{
  storage: {
    app: {
      options: {
        endpoint: 'https://s3.amazonaws.com',
        accessKeyId: 'myAccessKey',
        secretAccessKey: 'mySecretKey',
        s3ForcePathStyle: false,
        s3BucketEndpoint: false,
        bucketPrefix: 'myBucket', // myBucket1-100
        bucketStart: 1,
        bucketEnd: 100
      }
    }
  }
}
```

See AWS S3 for full options list: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#constructor-property

| Option | Type | Default | Desc |
| --- | --- | --- | --- |
| endpoint | string | `"https://s3.amazonaws.com"` | Endpoint of S3-compatible service |
| accessKeyId | string | (required) | Access key |
| secretAccessKey | string | (required) | Secret |
| s3ForcePathStyle | bool | `true` | Force path style |
| s3BucketEndpoint | bool | `false` | False if endpoint is the root API, not the bucket endpoint |
| bucketPrefix | string | (required) | Prefix of the final bucket name |
| bucketStart | number | `false` | If valid number, files will be sharded across buckets ranging from bucketStart to bucketEnd |
| bucketEnd | number | `false` | If valid number, files will be sharded across buckets ranging from bucketStart to bucketEnd |


### Secrets

Recommended to store your `secret` in blobby's **Secure Configuration**.

### Sharding

Your needs may vary, but leveraging `bucketStart` and `bucketEnd` to shard
your directories across multiple buckets is recommended to avoid scaling
limitations, be it storage, throughput, or otherwise. Even Amazon AWS S3
has per-bucket limitations.


## Initializing

If you plan to leverage [Sharding](#sharding), you can run blobby's
`initialize` command to pre-create the buckets for you to save time.

```
blobby initialize
```
