'use strict';

var test = require('tape')
var r = require('ramda')
var mChain = require('../')
var mCrypto = require('../../microstar-crypto')
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
      key: '3NPUITtu1D2sXKCYafDXPJVfl+Zii5WsL51adDjgUep+7kwEZ5LZwPdCBqxjvEzHccWtKsyTbNPSBxVtfef3Jw==',
      value: {
        chain_id: 'holiday-carols:2014',
        content: 'Laa',
        previous: 'LKMpfcd8RnoZyebGLj3vi+GsfNarHSXKcZH6XPnf1RpOOwgalRuf1nz+jyOzRJ52x+p2EopGn3T7oH/j9fAmZQ==',
        pub_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 2,
        signature: 'av/kuk31yYEk/MzE5bM28VzsGdLAezvQfL2woO9fy7Zr5G3xHGkuyF7yHsqibRdfyjcZV9kSocA0hqIzULJACw==',
        timestamp: 1418804138170,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'LWTQmsJ1E9fu+gSXDM03ckBXieL9/K8Jl2claIRcC6FFX5WYd1ojDsgo6KK1GafCinq2lAQlsIeVtU4RSpYL1w==',
      value: {
        chain_id: 'holiday-carols:2014',
        content: 'Fa',
        previous: null,
        pub_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 0,
        signature: 'Cs8s0zZgqE/Tp+DCFDXuMYA6mNUtPTFGf//5rENPCx37g3L7BFhz0pBJ06GFK5E1i3C6o5H9BgX/Ltppf5EFBQ==',
        timestamp: 1418804138168,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'XfTJe2J1lrYOTRJOQQh6UVsL3QFT2x3p7kpL6oPqpB4geGrg07FuLDRnNGtSKvIjGvA2FKanK62+METHMvn9Aw==',
      value: {
        chain_id: 'holiday-carols:2014',
        content: 'La',
        previous: 'jVCpXdW88KASKmZmtANpZURrGVB1YKyDlVO/oCjFalNg7KtfwxjmIkPhOrF9SRcM/MiM9+Wh13TZTOggDsKDzA==',
        pub_key: 'N3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=',
        sequence: 1,
        signature: 'vlOOUutmdXbty/qM4kUhSmgC7WnJ1Dvoutk9gfPe1sgHpULJxpjG0251nPGkmsPr7wjDtPKm6HPOK2s+1nLaCw==',
        timestamp: 1418804138169,
        type: 'holiday-carols:syllable'
      }
    }, {
      key: 'ÿpub_key,chain_id,sequenceÿN3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=ÿholiday-carols:2014ÿ0ÿLWTQmsJ1E9fu+gSXDM03ckBXieL9/K8Jl2claIRcC6FFX5WYd1ojDsgo6KK1GafCinq2lAQlsIeVtU4RSpYL1w==ÿ',
      value: 'LWTQmsJ1E9fu+gSXDM03ckBXieL9/K8Jl2claIRcC6FFX5WYd1ojDsgo6KK1GafCinq2lAQlsIeVtU4RSpYL1w=='
    }, {
      key: 'ÿpub_key,chain_id,sequenceÿN3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=ÿholiday-carols:2014ÿ1ÿXfTJe2J1lrYOTRJOQQh6UVsL3QFT2x3p7kpL6oPqpB4geGrg07FuLDRnNGtSKvIjGvA2FKanK62+METHMvn9Aw==ÿ',
      value: 'XfTJe2J1lrYOTRJOQQh6UVsL3QFT2x3p7kpL6oPqpB4geGrg07FuLDRnNGtSKvIjGvA2FKanK62+METHMvn9Aw=='
    }, {
      key: 'ÿpub_key,chain_id,sequenceÿN3DyaY1o1EmjPLUkRQRu41/g/xKe/CR/cCmatA78+zY=7XuCMMWN3y/r6DeVk7YGY8j/0rWyKm3TNv3S2cbmXKk=ÿholiday-carols:2014ÿ2ÿ3NPUITtu1D2sXKCYafDXPJVfl+Zii5WsL51adDjgUep+7kwEZ5LZwPdCBqxjvEzHccWtKsyTbNPSBxVtfef3Jw==ÿ',
      value: '3NPUITtu1D2sXKCYafDXPJVfl+Zii5WsL51adDjgUep+7kwEZ5LZwPdCBqxjvEzHccWtKsyTbNPSBxVtfef3Jw=='
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