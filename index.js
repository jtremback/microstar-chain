'use strict';

var mMessage = require('../microstar-message')
var llibrarian = require('../level-librarian')
var pull = require('pull-stream')

module.exports = {
  read: llibrarian.read,
  readOne: llibrarian.readOne,
  write: write,
  writeOne: llibrarian.makeWriteOne(write),
  copy: copy,
  validate: validate,
  createDocs: createDocs,
  sequential: sequential,
  indexes: [
    ['public_key', 'chain_id', 'sequence']
  ]
}

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
  var previous
  return pull(
    pull.asyncMap(function (message, callback) {
      if (!previous) {
        // Get previous message from db
        llibrarian.readOne(settings, {
          k: ['public_key', 'chain_id', 'sequence'],
          v: [settings.keys.public_key, message.chain_id],
          peek: 'last'
        }, function (err, prev) {
          if (prev) { prev = prev.value }
          mMessage.createEnvelope(settings, message, prev, function (err, message) {
            previous = message
            return callback(err, message)
          })
        })
      } else {
        mMessage.createEnvelope(settings, message, previous, function (err, message) {
          previous = message
          return callback(err, message)
        })
      }
    }),
    createDocs(settings),
    llibrarian.write(settings, callback)
  )
}

function read (settings, query) {
  return pull(
    llibrarian.read(settings, query),
    pull.map(function (item) {
      // Get out of doc format
      return item.value
    })
  )
}

// This outputs a sequential stream of messages suitable for serialization
// or validation.
function sequential (settings, public_key, chain_id, sequence) {
  var query
  if (sequence) {
    query = {
      k: ['public_key', 'chain_id', 'sequence'],
      v: [public_key, chain_id, [sequence, null]]
    }
  } else { // If no sequence arg, get all
    query = {
      k: ['public_key', 'chain_id', 'sequence'],
      v: [public_key, chain_id]
    }
  }

  return read(settings, query)
}

// Saves messages without making envelopes, but with validation
// against past messages
function copy (settings, initial, callback) {
  return pull(
    validate(settings, initial),
    createDocs(settings),
    llibrarian.write(settings, callback)
  )
}

function validate (settings, initial) {
  var previous
  return pull.asyncMap(function (message, callback) {
    mMessage.validate(settings, message, previous || initial, function (err, message) {
      previous = message
      callback(err, message)
    })
  })
}

function createDocs (settings) {
  return pull.asyncMap(function (message, callback) {
    mMessage.createDoc(settings, message, callback)
  })
}

