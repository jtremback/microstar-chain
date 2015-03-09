'use strict';

var test = require('tape')
var mChain = require('../')
var mCrypto = require('../../microstar-crypto')
var pull = require('pull-stream')
var pl = require('pull-level')
var async = require('async')
var crypto = require('crypto')
var clone = require('lodash.clone')
var stringify = require('stable-stringify')



// var level = require('level-test')()

var level = require('level')

var rimraf = require('rimraf')
rimraf.sync('./test1.db')
rimraf.sync('./test2.db')

var db = level('./test1.db', { valueEncoding: 'json' })
var db2 = level('./test2.db', { valueEncoding: 'json' })

// function dwrite (data) {
//   console.log(stringify(data))
// }
// var dump = require('level-dump')
// dump(db, dwrite, function (err) {
//   if (err) { throw err }
//   console.log('\n\n\n\n\n\n\n\n')
//   dump(db2, dwrite, function () {})
// })


var trace = require('get-trace')
function tracify (t) {
  function attach (func) {
    return function () {
      var args = []
      for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i])
      }
      args.push(trace(2))

      return func.apply(null, args)
    }
  }
  return {
    equal: attach(t.equal),
    deepEqual: attach(t.deepEqual),
    error: attach(t.error),
    end: t.end,
    plan: t.plan
  }
}

function sha1hex (input) {
  return crypto.createHash('sha1').update(JSON.stringify(input)).digest('hex')
}

mCrypto.keys('h4dfDIR+i3JfCw1T2jKr/SS/PJttebGfMMGwBvhOzS4=', function (err, keys) {
  tests(keys)
})

function tests (keys) {
  var settings = {
    crypto: mCrypto,
    keys: keys,
    db: db,
    index_defs: mChain.index_defs
  }

  var raw_messages = [{
    content: 'Fa',
    timestamp: 1418804138168,
    type: 'holiday-carols:syllable',
    chain_id: 'holiday-carols:2014'
  }, {
    content: 'La',
    timestamp: 1418804138169,
    type: 'holiday-carols:syllable',
    chain_id: 'holiday-carols:2014'
  }, {
    content: 'Laa',
    timestamp: 1418804138170,
    type: 'holiday-carols:syllable',
    chain_id: 'holiday-carols:2014'
  }]

  test('write', function (t) {
    t = tracify(t)
    async.series([
      // This writes a stream of two messages to the db.
      function (callback) {
        pull(
          pull.values([raw_messages[0], raw_messages[1]]),
          mChain.write(settings, callback)
        )
      },
      // This writes another, after the first two are done.
      function (callback) {
        setTimeout(function () {
          mChain.writeOne(settings, raw_messages[2], callback)
        }, 1000)
      }
    ], function (err) {
      if (err) { throw err }
      pull(
        pl.read(db),
        pull.collect(function (err, arr) {
          t.error(err)
          t.deepEqual(sha1hex(stringify(arr)), '83330ec925444693f1d34026cb00ed86b7652223')
          t.end()
        })
      )
    })
  })

  var messages = [{
    chain_id: 'holiday-carols:2014',
    content: 'Fa',
    previous: null,
    public_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
    sequence: 0,
    signature: '8qq7OacU+0dvBjgjwCR60l4PGekmr9Dd/F8XPmSBCvJdJ4bk9Raa0/AGVJx1r9ABIR4M19Qq7BQbw+aQGHNwAQ==',
    timestamp: 1418804138168,
    type: 'holiday-carols:syllable'
  },
  {
    chain_id: 'holiday-carols:2014',
    content: 'La',
    previous: 'svQZgkOrqLQb4JEO/WidhH/Doyn5AK6pGHHVWqmRaiFIG9qmtpBP2YDZMTyGfMeSTGxk0WoPz0QNMOaLUCZeqQ==',
    public_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
    sequence: 1,
    signature: 'pitLmr//o8tWcsD5qgj87aC024H/PsPeFzwmM9w8hrQdDqSRHnzdBc+KIoQ9Zwrqc4A+M++jo7dhcuUy9Xp4CQ==',
    timestamp: 1418804138169,
    type: 'holiday-carols:syllable'
  },
  {
    chain_id: 'holiday-carols:2014',
    content: 'Laa',
    previous: 'zcSN5BdLPyZMLwsN0NkOWXjNHbnss2YRRpNhdA63V+mliYjCTj3ySkbLIGcy4Jn9c5oHdQI8eMjdiHgkzZ7e3Q==',
    public_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
    sequence: 2,
    signature: 'WHu819414rDA3e4mgHijrcEGa7FEjpaC/FJMuu5fXzcEgqFe655tdUxbNEuZj9Jyon1WhLOJDGOHIJ8wsXY5AA==',
    timestamp: 1418804138170,
    type: 'holiday-carols:syllable'
  }]

  test('copy', function (t) {
    t = tracify(t)
    var _settings = clone(settings)
    _settings.db = db2

    async.series([
      // This writes a stream of two messages to the db.
      function (callback) {
        pull(
          pull.values([messages[0], messages[1]]),
          mChain.copy(_settings, callback)
        )
      },
      // This writes another, after the first two are done.
      function (callback) {
        setTimeout(function () {
          mChain.copyOne(_settings, messages[2], callback)
        }, 1000)
      }
    ], function (err) {
      if (err) { throw err }
      pull(
        pl.read(db2),
        pull.collect(function (err, arr) {
          t.error(err)
          t.deepEqual(sha1hex(stringify(arr)), '83330ec925444693f1d34026cb00ed86b7652223')
          t.end()
        })
      )
    })
  })

  test('sequential', function (t) {
    t = tracify(t)
    t.plan(2)

    pull(
      mChain.sequential(settings, settings.keys.public_key, 'holiday-carols:2014'),
      pull.collect(function (err, arr) {
        if (err) { throw err }
        t.deepEqual(arr, [messages[0], messages[1], messages[2]], 'Sequential without supplied sequence')
      })
    )

    pull(
      mChain.sequential(settings, settings.keys.public_key, 'holiday-carols:2014', 1),
      pull.collect(function (err, arr) {
        if (err) { throw err }
        t.deepEqual(arr, [messages[1], messages[2]], 'Sequential with supplied sequence')
      })
    )
  })
}