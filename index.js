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
  // createSequences: createSequences,
  // createEnvelopes: createEnvelopes,
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

// // Formats messages and then writes them to db
// function write (settings, callback) {
//   return pull(
//     createSequences(settings),
//     createEnvelopes(settings),
//     createDocs(settings),
//     llibrarian.write(settings, callback)
//   )
// }

// Formats messages and then writes them to db
function write (settings, a, b) {
  var previous
  var initial
  var callback
  if (typeof a === 'function') {
    callback = a
  } else {
    initial = a
    callback = b
  }
  return pull(
    pull.asyncMap(function (message, callback) {
      if (!previous && !initial) {
        // Get previous message from db
        llibrarian.readOne(settings, {
          k: ['public_key', 'chain_id', 'sequence'],
          v: [settings.keys.public_key, message.chain_id],
          peek: 'last'
        }, function (err, prev) {
          if (prev) { prev = prev.value }
          format(settings, message, prev || initial, function (err, message) {
            previous = message
            callback(err, message)
          })
        })
      } else {
        format(settings, message, previous || initial, function (err, message) {
          previous = message
          callback(err, message)
        })
      }
    }),
    createDocs(settings),
    llibrarian.write(settings, callback)
  )
}

function format (settings, message, prev, callback) {
  message = mMessage.createSequence(settings, message, prev)
  mMessage.createEnvelope(settings, message, prev, function (err, message) {
    prev = message
    return callback(err, message, prev)
  })
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
  var prev
  return pull.asyncMap(function (message, callback) {
    mMessage.validate(settings, message, prev || initial, function (err, message) {
      prev = message
      callback(err, message)
    })
  })
}

// function createEnvelopes (settings, initial) {
//   var prev
//   return pull.asyncMap(function (message, callback) {
//     mMessage.createEnvelope(settings, message, prev || initial, function (err, message) {
//       prev = message
//       return callback(err, message)
//     })
//   })
// }

// function createSequences (settings, initial) {
//   var prev
//   return pull.map(function (message) {
//     message = mMessage.createSequence(settings, message, prev)
//     prev = message
//     return message
//   })
// }

// function createSequences (settings, initial) {
//   var prev
//   return pull.asyncMap(function (message, callback) {
//     // prev = mMessage.createSequence(settings, message, prev)
//     // return prev

//     if (!prev && !initial) {
//       // Get previous message from db
//       llibrarian.readOne(settings, {
//         k: ['pub_key', 'chain_id', 'sequence'],
//         v: [settings.keys.publicKey, message.chain_id],
//         peek: 'last'
//       }, function (err, last) {
//         message = mMessage.createSequence(settings, message, last)
//         prev = message
//         callback(err, message)
//       })
//     } else {
//       message = mMessage.createSequence(settings, message, prev || initial)
//       callback(null, message)
//     }
//   })
// }

function createDocs (settings) {
  return pull.asyncMap(function (message, callback) {
    mMessage.createDoc(settings, message, callback)
  })
}

