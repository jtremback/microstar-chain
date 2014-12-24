'use strict';

var test = require('tape')
var r = require('ramda')
var mChain = require('../')
var mCrypto = require('microstar-crypto')
var rimraf = require('rimraf')
var level = require('level')
var pull = require('pull-stream')
var pl = require('pull-level')

rimraf.sync('./test.db')
var db = level('./test.db', { valueEncoding: 'json' })

var dbContents

mCrypto.keys('h4dfDIR+i3JfCw1T2jKr/SS/PJttebGfMMGwBvhOzS4=', function (err, keys) {
  tests(mChain({
    crypto: mCrypto,
    keys: keys,
    db: db,
    indexes: mChain.indexes
  }))
})

function tests (mChain) {
  var messages = [{
    content: 'Fa',
    timestamp: 1418804138168,
    type: 'holiday-carols:syllable',
    feed_id: 'holiday-carols:2014'
  }, {
    content: 'La',
    timestamp: 1418804138168,
    type: 'holiday-carols:syllable',
    feed_id: 'holiday-carols:2014'
  }, {
    content: 'Laa',
    timestamp: 1418804138168,
    type: 'holiday-carols:syllable',
    feed_id: 'holiday-carols:2014'
  }]

  test('write', function (t) {
    pull(
      pull.values(messages),
      mChain.write(function (err) {
        t.error(err)

        pull(
          pl.read(db),
          pull.collect(function (err, arr) {
            t.error(err)
            t.deepEqual(arr, dbContents, '.write(db, indexes)')
            t.end()
          })
        )
      })
    )


    dbContents = [{
      key: '9GWLN2OmBdXK8tKK0/jP0WxsxF40Qx6lXFXjNilOZC4rglLwUNGg7UiLWUlzBXExyFWxNOiohf8NTRtHEZ1QPQ==',
      value: {
        content: 'Fa',
        feed_id: 'holiday-carols:2014',
        previous: null,
        pub_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 0,
        signature: '0QdzchEK74ukRqtV5OGx0Jvtzc+ddpOg2x+5MzP0v6B0A64xeBlJB91QjSobSi0y++c9uhq1AYWOaRHbZ+Q/CA==',
        timestamp: 1418804138168,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'KG/P0BYfYtwD1habhWabfyMkAzfLFLcHfj8hOu8L6ebvMMrq2yDdhrgOjJOxh1Ozfpo4k4BVZeoewTLBXGUz0w==',
      value: {
        content: 'La',
        feed_id: 'holiday-carols:2014',
        previous: null,
        pub_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 1,
        signature: '7KvWMmmfuZPVX1jU35IKsduUZILk3z6osVWZACJfObrzMqhN/ByLgj5AWIQb5RroqH1JNxZVstp6SB3vrLbiDA==',
        timestamp: 1418804138168,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'qRkq8aMERuTU/Lp+of0vKv5nS3roiUTgcF9WIUWBRrE9NXDFOin8lexIpMYmAwI3CUz5qb+6/a29nAPZWYLWfQ==',
      value: {
        content: 'Laa',
        feed_id: 'holiday-carols:2014',
        previous: null,
        pub_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 2,
        signature: 'JQIBYiQekW7Gx3hVUJU9PpG2bKNXj8VglQe/DJ8P3GCd3KODhPiDsH67dtx4rqt5Psgka131TItfbn4jFDZFBQ==',
        timestamp: 1418804138168,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'ÿfeed_id,$latestÿholiday-carols:2014ÿÿ',
      value: 'qRkq8aMERuTU/Lp+of0vKv5nS3roiUTgcF9WIUWBRrE9NXDFOin8lexIpMYmAwI3CUz5qb+6/a29nAPZWYLWfQ=='
    }]
  })

  test('copy', function (t) {
    var initial = dbContents[0].value
    var values = [dbContents[1].value, dbContents[2].value]

    pull(
      pull.values(values),
      mChain.copy(initial, function (err) {
        t.error(err)

        pull(
          pl.read(db),
          pull.collect(function (err, arr) {
            t.error(err)
            t.deepEqual(arr, dbContents, '.write(db, indexes)')
            t.end()
          })
        )
      })
    )
  })

//   test('read', function (t) {

//   })
}