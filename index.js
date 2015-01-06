'use strict';

var mMessage = require('../microstar-message')
var llibrarian = require('../level-librarian')
var pairs = require('pull-pairs')
var pull = require('pull-stream')

module.exports = function (settings) {
  return {
    read: llibrarian.read.bind(null, settings),
    write: write.bind(null, settings),
    copy: copy.bind(null, settings),
    sequential: sequential.bind(null, settings)
  }
}

module.exports.read = llibrarian.read
module.exports.write = write
module.exports.copy = copy
module.exports.sequential = sequential

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
    format(settings),
    llibrarian.write(settings, callback)
  )
}

// Saves messages without formatting, but with validation
// against past messages
function copy (settings, initial, callback) {
  return pull(
    validate(settings, initial),
    pull.asyncMap(function (message, callback) {
      mMessage.makeDoc(settings, message, callback)
    }),
    llibrarian.write(settings, callback)
  )
}

// This outputs a sequential stream of messages suitable for serialization
// or validation.
function sequential (settings, pub_key, chain_id, initial) {
  var query
  // If no initial arg, get all
  if (initial) {
    query = {
      k: ['pub_key', 'chain_id', 'sequence'],
      v: [pub_key, chain_id, [initial, null]]
    }
  } else {
    query = {
      k: ['pub_key', 'chain_id', 'sequence'],
      v: [pub_key, chain_id]
    }
  }

  return pull(
    llibrarian.read(settings, query),
    // Strip off key for transmission
    pull.map(function (doc) {
      return doc.value
    })
  )
}

// This will error on the first invalid message
function validate (settings, initial) {
  return pull(
    pairs(function (a, b) {
      return [a, b]
    }),
    // Filter out pair that has missing b
    pull.filter(function (pair) {
      return pair[1]
    }),
    pull.asyncMap(function (pair, callback) {
      // If it is the first message in stream (pair[0] is null), use supplied
      // initial. If no supplied initial, is considered first in chain.
      mMessage.validate(settings, pair[1], pair[0] || initial, function (err) {
        return callback(err, pair[1])
      })
    })
  )
}

// This formats a stream of messages. There are three possibilities for the
// initial message. Supplied `initial` argument, previous message from db, or
// no previous message, which will start the chain.
function format (settings, initial) {
  var last
  return pull.asyncMap(function (message, callback) {
    if (!last) { // If this is the first time this stream has run
      if (initial) { // If initial message is supplied
        mMessage.format(settings, message, initial, function (err, message) {
          last = message
          callback(err, message)
        })
      } else { // Get previous message from db
        llibrarian.readOne(settings, {
          k: ['pub_key', 'chain_id', 'sequence'],
          v: [settings.keys.publicKey, message.chain_id],
          peek: 'last'
        }, function (err, prev) {
          // Message will either be formatted with the prev message from db,
          // or with null if it does not exist (starting a new chain)
          mMessage.format(settings, message, prev && prev.value, function (err, message) {
            last = message
            callback(err, message)
          })
        })
      }
    } else { // If the stream has run before, use message from then
      mMessage.format(settings, message, last, function (err, message) {
        last = message
        callback(err, message)
      })
    }
  })
}
