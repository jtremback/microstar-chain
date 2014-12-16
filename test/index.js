'use strict';

var test = require('tape')
var mFeed = require('../')
var mCrypto = require('microstar-crypto')
var rimraf = require('rimraf')
var level = require('level')
var pull = require('pull-stream')
var pl = require('pull-level')

rimraf.sync('./test.db')
var db = level('./test.db', { valueEncoding: 'json' })

mCrypto.keys('h4dfDIR+i3JfCw1T2jKr/SS/PJttebGfMMGwBvhOzS4=', function (err, keys) {
  tests(mFeed({
    crypto: mCrypto,
    keys: keys,
    db: db
  }))
})

function tests (mFeed) {
  var messages = [{
    content: 'Fa'
  }, {
    content: 'La'
  }, {
    content: 'La'
  }]

  test('write', function (t) {
    pull(
      pull.values(messages),
      mFeed.write({
        type: 'holiday-carols:syllable',
        feed_id: 'holiday-carols:2014'
      }, function (err) {
        t.error(err)
        debugger
        checkDB()
      })
    )

    function checkDB () {
      pull(
        pl.read(db),
        pull.collect(function (err, arr) {
          t.error(err)
          t.deepEqual(arr, dbContents, '.write(db, indexes)')
          t.end()
        })
      )
    }

    var dbContents = []
  })

  test('save', function (t) {

  })

  test('read', function (t) {

  })
}