'use strict';

var mMessage = require('../microstar-message')
var mChain = require('../microstar-chain')
var llibrarian = require('../level-librarian')
var pairs = require('pull-pairs')
var pull = require('pull-stream')
var r = require('ramda')

module.exports = function (settings) {
  return {
    read: llibrarian.read.bind(null, settings),
    write: write.bind(null, settings),
    copy: copy.bind(null, settings)
  }
}

module.exports.read = llibrarian.read
module.exports.write = write
module.exports.copy = copy

module.exports.indexes = [
  ['pub_key', 'chain_id', 'sequence']
]

// settings = {
//   crypto: JS,
//   keys: JS,
//   db: db
// }

// message = {
//   content: JSON,
//   type: String,
//   chain_id: String
// }

// Formats messages and then writes them to db
function write (settings, callback) {
  return pull(
    formatMessages(settings),
    llibrarian.write(settings, callback)
  )
}

// Returns stream of messages in order
function streamLater (settings) {
  return pull(
    pull.map(function (message) {
      // Gather all messages later than latest
      return mChain.read(settings, {
        k: ['pub_key', 'chain_id', 'sequence'],
        v: [message.pub_key, message.chain_id, [message.sequence, null]]
      })
    }),
    // Unwind stream of streams into single stream
    unwind()
  )
}

// Saves messages without formatting, but with validation
// against past messages
function copy (settings, initial, callback) {
  return pull(
    validateMessages(settings, initial),
    pull.asyncMap(function (message, callback) {
      mMessage.makeDoc(settings, message, callback)
    }),
    llibrarian.write(settings, callback)
  )
}

// This will stop a stream on the first invalid message
function validateMessages (settings) {
  return pull(
    // Make pairs
    pairs(function (a, b) {
      return [a, b]
    }),
    // Filter out pair that has missing b. I feel like this is janky.
    pull.filter(function (pair) {
      return pair[1]
    }),
    pull.asyncMap(function (pair, callback) {
      // If sequence = 0, then validate with null
      if (!pair[0]) {
        mMessage.validate(settings, pair[1], null, function (err) {
          return callback(err, pair[1])
        })

      // If first in chain, in this stream, validate with prev from db.
      } else if (pair[0].chain_id !== pair[1].chain_id) {
        mChain.readOne(settings, {
          k: ['pub_key', 'chain_id', 'sequence'],
          v: [pair[1].pub_key, pair[1].chain_id, pair[1].sequence]
        }, function (err, doc) {
          mMessage.validate(settings, pair[1], doc.value, function (err) {
            return callback(err, pair[1])
          })
        })

      // If no, validate with previous in stream
      } else {
        mMessage.validate(settings, pair[1], pair[0], function (err) {
          return callback(err, pair[1])
        })
      }
    })
  )
}

function formatMessages (settings) {
  var last
  return pull.asyncMap(function (message, callback) {
    if (!last) {
      // Get previous message from db
      llibrarian.readOne(settings, {
        k: ['pub_key', 'chain_id', 'sequence'],
        v: [settings.keys.publicKey, message.chain_id],
        peek: 'last'
      }, function (err, prev) {
        mMessage.create(settings, message, prev.value, function (err, message) {
          last = message
          callback(err, message)
        })
      })
    } else {
      mMessage.create(settings, message, last, function (err, message) {
        last = message
        callback(err, message)
      })
    }
  })
}
