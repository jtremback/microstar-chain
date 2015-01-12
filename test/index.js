'use strict';

var test = require('tape')
var mChain = require('../')
var mCrypto = require('../../microstar-crypto')
var level = require('level-test')()
var pull = require('pull-stream')
var pl = require('pull-level')


var dbContents
var db1 = level('./test1.db', { valueEncoding: 'json' })

mCrypto.keys('h4dfDIR+i3JfCw1T2jKr/SS/PJttebGfMMGwBvhOzS4=', function (err, keys) {
  tests(keys)
})

function tests (keys) {
  var settings = {
    crypto: mCrypto,
    keys: keys,
    db: db1,
    indexes: mChain.indexes
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

    pull(
      pull.values(raw_messages),
      mChain.write(settings, function (err) {
        t.error(err)

        pull(
          pl.read(db1),
          pull.collect(function (err, arr) {
            t.error(err)
            t.deepEqual(arr, dbContents, '.write(db, indexes)')
            t.end()
          })
        )
      })
    )


    dbContents = [{
      key: 'svQZgkOrqLQb4JEO/WidhH/Doyn5AK6pGHHVWqmRaiFIG9qmtpBP2YDZMTyGfMeSTGxk0WoPz0QNMOaLUCZeqQ==',
      value: {
        chain_id: 'holiday-carols:2014',
        content: 'Fa',
        previous: null,
        public_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 0,
        signature: '8qq7OacU+0dvBjgjwCR60l4PGekmr9Dd/F8XPmSBCvJdJ4bk9Raa0/AGVJx1r9ABIR4M19Qq7BQbw+aQGHNwAQ==',
        timestamp: 1418804138168,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'vB2XSqXzalwgQkaksvcz5S9eI6emIQ4iSRMlDJAt/obxuBMUxp1MJUJJBotLtHYK0LTTSuuuV9aAW1oY7yhYcw==',
      value: {
        chain_id: 'holiday-carols:2014',
        content: 'Laa',
        previous: 'zcSN5BdLPyZMLwsN0NkOWXjNHbnss2YRRpNhdA63V+mliYjCTj3ySkbLIGcy4Jn9c5oHdQI8eMjdiHgkzZ7e3Q==',
        public_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 2,
        signature: 'WHu819414rDA3e4mgHijrcEGa7FEjpaC/FJMuu5fXzcEgqFe655tdUxbNEuZj9Jyon1WhLOJDGOHIJ8wsXY5AA==',
        timestamp: 1418804138170,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'zcSN5BdLPyZMLwsN0NkOWXjNHbnss2YRRpNhdA63V+mliYjCTj3ySkbLIGcy4Jn9c5oHdQI8eMjdiHgkzZ7e3Q==',
      value: {
        chain_id: 'holiday-carols:2014',
        content: 'La',
        previous: 'svQZgkOrqLQb4JEO/WidhH/Doyn5AK6pGHHVWqmRaiFIG9qmtpBP2YDZMTyGfMeSTGxk0WoPz0QNMOaLUCZeqQ==',
        public_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 1,
        signature: 'pitLmr//o8tWcsD5qgj87aC024H/PsPeFzwmM9w8hrQdDqSRHnzdBc+KIoQ9Zwrqc4A+M++jo7dhcuUy9Xp4CQ==',
        timestamp: 1418804138169,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'ÿpublic_key,chain_id,sequenceÿN3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=ÿholiday-carols:2014ÿ0ÿsvQZgkOrqLQb4JEO/WidhH/Doyn5AK6pGHHVWqmRaiFIG9qmtpBP2YDZMTyGfMeSTGxk0WoPz0QNMOaLUCZeqQ==ÿ',
      value: 'svQZgkOrqLQb4JEO/WidhH/Doyn5AK6pGHHVWqmRaiFIG9qmtpBP2YDZMTyGfMeSTGxk0WoPz0QNMOaLUCZeqQ=='
    }, {
      key: 'ÿpublic_key,chain_id,sequenceÿN3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=ÿholiday-carols:2014ÿ1ÿzcSN5BdLPyZMLwsN0NkOWXjNHbnss2YRRpNhdA63V+mliYjCTj3ySkbLIGcy4Jn9c5oHdQI8eMjdiHgkzZ7e3Q==ÿ',
      value: 'zcSN5BdLPyZMLwsN0NkOWXjNHbnss2YRRpNhdA63V+mliYjCTj3ySkbLIGcy4Jn9c5oHdQI8eMjdiHgkzZ7e3Q=='
    }, {
      key: 'ÿpublic_key,chain_id,sequenceÿN3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=ÿholiday-carols:2014ÿ2ÿvB2XSqXzalwgQkaksvcz5S9eI6emIQ4iSRMlDJAt/obxuBMUxp1MJUJJBotLtHYK0LTTSuuuV9aAW1oY7yhYcw==ÿ',
      value: 'vB2XSqXzalwgQkaksvcz5S9eI6emIQ4iSRMlDJAt/obxuBMUxp1MJUJJBotLtHYK0LTTSuuuV9aAW1oY7yhYcw=='
    }]
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

  test('validate', function (t) {
    t.plan(2)


    pull(
      pull.values([messages[1], messages[2]]),
      mChain.validate(settings, messages[0]),
      pull.collect(function (err, arr) {
        if (err) { throw err }
        t.deepEqual(arr, [messages[1], messages[2]], 'Partial chain with supplied initial message.')
      })
    )

    pull(
      pull.values([messages[0], messages[1], messages[2]]),
      mChain.validate(settings),
      pull.collect(function (err, arr) {
        if (err) { throw err }
        t.deepEqual(arr, [messages[0], messages[1], messages[2]], 'Partial chain without supplied initial message.')
      })
    )
  })

  test('sequential', function (t) {
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