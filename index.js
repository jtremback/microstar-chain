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
  makeEnvelopes: makeEnvelopes,
  makeDocs: makeDocs,
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
  return pull(
    makeEnvelopes(settings),
    makeDocs(settings),
    llibrarian.write(settings, callback)
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

  return llibrarian.read(settings, query)
}

// Saves messages without making envelopes, but with validation
// against past messages
function copy (settings, initial, callback) {
  return pull(
    validate(settings, initial),
    makeDocs(settings),
    llibrarian.write(settings, callback)
  )
}

function makeDocs (settings) {
  return pull.asyncMap(function (message, callback) {
    mMessage.makeDoc(settings, message, callback)
  })
}

function makeEnvelopes (settings, initial) {
  var last
  return pull.asyncMap(function (message, callback) {
    mMessage.makeEnvelope(settings, message, last || initial, function (err, message) {
      last = message
      return callback(err, message)
    })
  })
}

function validate (settings, initial) {
  var last
  return pull.asyncMap(function (message, callback) {
    mMessage.validate(settings, message, last || initial, function (err, message) {
      last = message
      callback(err, message)
    })
  })
}
