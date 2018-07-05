/*jslint node: true */
"use strict";

var _stringify = require('babel-runtime/core-js/json/stringify');

var _stringify2 = _interopRequireDefault(_stringify);

var _regenerator = require('babel-runtime/regenerator');

var _regenerator2 = _interopRequireDefault(_regenerator);

var _getIterator2 = require('babel-runtime/core-js/get-iterator');

var _getIterator3 = _interopRequireDefault(_getIterator2);

var _asyncToGenerator2 = require('babel-runtime/helpers/asyncToGenerator');

var _asyncToGenerator3 = _interopRequireDefault(_asyncToGenerator2);

var updateHistory = function () {
	var _ref = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee2(address) {
		var trans, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, addr, result, update_trans, insert_trans, _iteratorNormalCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, tran, unit;

		return _regenerator2.default.wrap(function _callee2$(_context2) {
			while (1) {
				switch (_context2.prev = _context2.next) {
					case 0:
						// let info = await hashnethelper.getInfo();
						// console.log(JSON.stringify(info));
						// return;
						trans = [];
						_iteratorNormalCompletion = true;
						_didIteratorError = false;
						_iteratorError = undefined;
						_context2.prev = 4;
						_iterator = (0, _getIterator3.default)(address);

					case 6:
						if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
							_context2.next = 15;
							break;
						}

						addr = _step.value;
						_context2.next = 10;
						return hashnethelper.getTransactionHistory(addr);

					case 10:
						result = _context2.sent;

						if (result.length > 0) {
							trans = trans.concat(result);
						}

					case 12:
						_iteratorNormalCompletion = true;
						_context2.next = 6;
						break;

					case 15:
						_context2.next = 21;
						break;

					case 17:
						_context2.prev = 17;
						_context2.t0 = _context2['catch'](4);
						_didIteratorError = true;
						_iteratorError = _context2.t0;

					case 21:
						_context2.prev = 21;
						_context2.prev = 22;

						if (!_iteratorNormalCompletion && _iterator.return) {
							_iterator.return();
						}

					case 24:
						_context2.prev = 24;

						if (!_didIteratorError) {
							_context2.next = 27;
							break;
						}

						throw _iteratorError;

					case 27:
						return _context2.finish(24);

					case 28:
						return _context2.finish(21);

					case 29:
						if (!(trans.length === 0)) {
							_context2.next = 33;
							break;
						}

						return _context2.abrupt('return');

					case 33:
						update_trans = [];
						insert_trans = [];
						_iteratorNormalCompletion2 = true;
						_didIteratorError2 = false;
						_iteratorError2 = undefined;
						_context2.prev = 38;
						_iterator2 = (0, _getIterator3.default)(trans);

					case 40:
						if (_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done) {
							_context2.next = 49;
							break;
						}

						tran = _step2.value;
						_context2.next = 44;
						return db.first("select * from units where unit = ?", tran.unitId);

					case 44:
						unit = _context2.sent;

						if (unit && unit.is_stable == 0) {
							update_trans.push(tran.unitId);
						} else if (!unit) {
							insert_trans.push(tran.unitId);
						}

					case 46:
						_iteratorNormalCompletion2 = true;
						_context2.next = 40;
						break;

					case 49:
						_context2.next = 55;
						break;

					case 51:
						_context2.prev = 51;
						_context2.t1 = _context2['catch'](38);
						_didIteratorError2 = true;
						_iteratorError2 = _context2.t1;

					case 55:
						_context2.prev = 55;
						_context2.prev = 56;

						if (!_iteratorNormalCompletion2 && _iterator2.return) {
							_iterator2.return();
						}

					case 58:
						_context2.prev = 58;

						if (!_didIteratorError2) {
							_context2.next = 61;
							break;
						}

						throw _iteratorError2;

					case 61:
						return _context2.finish(58);

					case 62:
						return _context2.finish(55);

					case 63:
						_context2.next = 65;
						return mutex.lock(["write"], function () {
							var _ref2 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee(unlock) {
								var i_bool, _iteratorNormalCompletion3, _didIteratorError3, _iteratorError3, _iterator3, _step3, unitId, unit, i_result;

								return _regenerator2.default.wrap(function _callee$(_context) {
									while (1) {
										switch (_context.prev = _context.next) {
											case 0:
												if (!(update_trans.length > 0)) {
													_context.next = 3;
													break;
												}

												_context.next = 3;
												return db.execute("update units set is_stable = 1 where unit in (?)", update_trans);

											case 3:
												i_bool = false;

												if (!(insert_trans.length > 0)) {
													_context.next = 36;
													break;
												}

												_iteratorNormalCompletion3 = true;
												_didIteratorError3 = false;
												_iteratorError3 = undefined;
												_context.prev = 8;
												_iterator3 = (0, _getIterator3.default)(insert_trans);

											case 10:
												if (_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done) {
													_context.next = 22;
													break;
												}

												unitId = _step3.value;
												_context.next = 14;
												return hashnethelper.getUnitInfo(unitId);

											case 14:
												unit = _context.sent;
												_context.next = 17;
												return insertHistory(unit.unit);

											case 17:
												i_result = _context.sent;

												if (!i_result) {
													i_bool = true;
												}

											case 19:
												_iteratorNormalCompletion3 = true;
												_context.next = 10;
												break;

											case 22:
												_context.next = 28;
												break;

											case 24:
												_context.prev = 24;
												_context.t0 = _context['catch'](8);
												_didIteratorError3 = true;
												_iteratorError3 = _context.t0;

											case 28:
												_context.prev = 28;
												_context.prev = 29;

												if (!_iteratorNormalCompletion3 && _iterator3.return) {
													_iterator3.return();
												}

											case 31:
												_context.prev = 31;

												if (!_didIteratorError3) {
													_context.next = 34;
													break;
												}

												throw _iteratorError3;

											case 34:
												return _context.finish(31);

											case 35:
												return _context.finish(28);

											case 36:
												unlock();
												if (update_trans.length > 0 || i_bool) {
													eventBus.emit('my_transactions_became_stable');
												}

											case 38:
											case 'end':
												return _context.stop();
										}
									}
								}, _callee, this, [[8, 24, 28, 36], [29,, 31, 35]]);
							}));

							return function (_x2) {
								return _ref2.apply(this, arguments);
							};
						}());

					case 65:
					case 'end':
						return _context2.stop();
				}
			}
		}, _callee2, this, [[4, 17, 21, 29], [22,, 24, 28], [38, 51, 55, 63], [56,, 58, 62]]);
	}));

	return function updateHistory(_x) {
		return _ref.apply(this, arguments);
	};
}();

var insertHistory = function () {
	var _ref3 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee3(objUnit) {
		var cmds, fields, values, params, bGenesis, arrAuthorAddresses, i, author, definition_chash, path, message, text_payload, payload, denomination, j, input, type, src_unit, src_message_index, src_output_index, from_main_chain_index, to_main_chain_index, is_unique, address, output;
		return _regenerator2.default.wrap(function _callee3$(_context3) {
			while (1) {
				switch (_context3.prev = _context3.next) {
					case 0:
						console.log("\nsaving unit " + objUnit);
						console.log((0, _stringify2.default)(objUnit));
						cmds = [];
						fields = "unit, version, alt, headers_commission, payload_commission, sequence, content_hash,is_stable";
						values = "?,?,?,?,?,?,?,?";
						params = [objUnit.unit, objUnit.version, objUnit.alt, objUnit.headers_commission || 0, objUnit.payload_commission || 0, 'good', objUnit.content_hash, 1];

						if (conf.bLight) {
							fields += ", main_chain_index, creation_date";
							values += ",?," + db.getFromUnixTime("?");
							params.push(objUnit.main_chain_index, objUnit.timestamp);
						}
						db.addCmd.apply(db, [cmds, "INSERT INTO units (" + fields + ") VALUES (" + values + ")"].concat(params));

						bGenesis = storage.isGenesisUnit(objUnit.unit);

						if (bGenesis) {
							db.addCmd(cmds, "UPDATE units SET is_on_main_chain=1, main_chain_index=0, is_stable=1, level=0, witnessed_level=0 \n\
					WHERE unit=?", objUnit.unit);
						}

						arrAuthorAddresses = [];

						for (i = 0; i < objUnit.authors.length; i++) {
							author = objUnit.authors[i];

							arrAuthorAddresses.push(author.address);
							definition_chash = null;

							db.addCmd(cmds, "INSERT INTO unit_authors (unit, address, definition_chash) VALUES(?,?,?)", objUnit.unit, author.address, definition_chash);
							if (bGenesis) db.addCmd(cmds, "UPDATE unit_authors SET _mci=0 WHERE unit=?", objUnit.unit);
							if (!objUnit.content_hash) {
								for (path in author.authentifiers) {
									db.addCmd(cmds, "INSERT INTO authentifiers (unit, address, path, authentifier) VALUES(?,?,?,?)", objUnit.unit, author.address, path, author.authentifiers[path]);
								}
							}
						}

						if (!objUnit.content_hash) {
							for (i = 0; i < objUnit.messages.length; i++) {
								message = objUnit.messages[i];
								text_payload = null;

								if (message.app === "text") {
									text_payload = message.payload;
								} else if (message.app === "data" || message.app === "profile" || message.app === "attestation" || message.app === "definition_template") {
									text_payload = (0, _stringify2.default)(message.payload);
								}
								db.addCmd(cmds, "INSERT INTO messages \n\
					(unit, message_index, app, payload_hash, payload_location, payload, payload_uri, payload_uri_hash) VALUES(?,?,?,?,?,?,?,?)", objUnit.unit, i, message.app, message.payload_hash, message.payload_location, text_payload, message.payload_uri, message.payload_uri_hash);
							}
						}

						for (i = 0; i < objUnit.messages.length; i++) {
							message = objUnit.messages[i];
							payload = message.payload;
							denomination = payload.denomination || 1;

							for (j = 0; j < payload.inputs.length; j++) {
								input = payload.inputs[j];
								type = input.type || "transfer";
								src_unit = type === "transfer" ? input.unit : null;
								src_message_index = type === "transfer" ? input.message_index : null;
								src_output_index = type === "transfer" ? input.output_index : null;
								from_main_chain_index = type === "witnessing" || type === "headers_commission" ? input.from_main_chain_index : null;
								to_main_chain_index = type === "witnessing" || type === "headers_commission" ? input.to_main_chain_index : null;
								is_unique = 1;
								address = arrAuthorAddresses.length === 1 ? arrAuthorAddresses[0] : input.address;

								db.addCmd(cmds, "INSERT INTO inputs \n\
				(unit, message_index, input_index, type, \n\
				src_unit, src_message_index, src_output_index, \
				from_main_chain_index, to_main_chain_index, \n\
				denomination, amount, serial_number, \n\
				asset, is_unique, address) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", objUnit.unit, i, j, type, src_unit, src_message_index, src_output_index, from_main_chain_index, to_main_chain_index, denomination, input.amount, input.serial_number, payload.asset, is_unique, address);
							}
							for (j = 0; j < payload.outputs.length; j++) {
								output = payload.outputs[j];

								db.addCmd(cmds, "INSERT INTO outputs \n\
					(unit, message_index, output_index, address, amount, asset, denomination, is_serial) VALUES(?,?,?,?,?,?,?,1)", objUnit.unit, i, j, output.address, parseInt(output.amount), payload.asset, denomination);
							}
						}

						_context3.next = 16;
						return db.executeTrans(cmds);

					case 16:
						return _context3.abrupt('return', _context3.sent);

					case 17:
					case 'end':
						return _context3.stop();
				}
			}
		}, _callee3, this);
	}));

	return function insertHistory(_x3) {
		return _ref3.apply(this, arguments);
	};
}();

// fixes is_spent in case units were received out of order


function _interopRequireDefault(obj) {
	return obj && obj.__esModule ? obj : { default: obj };
}

var async = require('async');
var storage = require('./storage.js');
var objectHash = require("./object_hash.js");
var db = require('./db.js');
var mutex = require('./mutex.js');
var constants = require("./constants.js");
var graph = require('./graph.js');
var writer = require('./writer.js');
var validation = require('./validation.js');
var witnessProof = require('./witness_proof.js');
var ValidationUtils = require("./validation_utils.js");
var parentComposer = require('./parent_composer.js');
var breadcrumbs = require('./breadcrumbs.js');
var eventBus = require('./event_bus.js');
var device = require('./device.js');
var MAX_HISTORY_ITEMS = 1000;
var hashnethelper = require('./hashnethelper');
var conf = require("./conf.js");
// unit's MC index is earlier_mci
function buildProofChain(later_mci, earlier_mci, unit, arrBalls, onDone) {
	if (earlier_mci === null) throw Error("earlier_mci=null, unit=" + unit);
	if (later_mci === earlier_mci) return buildLastMileOfProofChain(earlier_mci, unit, arrBalls, onDone);
	buildProofChainOnMc(later_mci, earlier_mci, arrBalls, function () {
		buildLastMileOfProofChain(earlier_mci, unit, arrBalls, onDone);
	});
}

// later_mci is already known and not included in the chain
function buildProofChainOnMc(later_mci, earlier_mci, arrBalls, onDone) {

	function addBall(mci) {
		if (mci < 0) throw Error("mci<0, later_mci=" + later_mci + ", earlier_mci=" + earlier_mci);
		db.query("SELECT unit, ball, content_hash FROM units JOIN balls USING(unit) WHERE main_chain_index=? AND is_on_main_chain=1", [mci], function (rows) {
			if (rows.length !== 1) throw Error("no prev chain element? mci=" + mci + ", later_mci=" + later_mci + ", earlier_mci=" + earlier_mci);
			var objBall = rows[0];
			if (objBall.content_hash) objBall.is_nonserial = true;
			delete objBall.content_hash;
			db.query("SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=balls.unit WHERE child_unit=? ORDER BY ball", [objBall.unit], function (parent_rows) {
				if (parent_rows.some(function (parent_row) {
					return !parent_row.ball;
				})) throw Error("some parents have no balls");
				if (parent_rows.length > 0) objBall.parent_balls = parent_rows.map(function (parent_row) {
					return parent_row.ball;
				});
				db.query("SELECT ball, main_chain_index \n\
						FROM skiplist_units JOIN units ON skiplist_unit=units.unit LEFT JOIN balls ON units.unit=balls.unit \n\
						WHERE skiplist_units.unit=? ORDER BY ball", [objBall.unit], function (srows) {
					if (srows.some(function (srow) {
						return !srow.ball;
					})) throw Error("some skiplist units have no balls");
					if (srows.length > 0) objBall.skiplist_balls = srows.map(function (srow) {
						return srow.ball;
					});
					arrBalls.push(objBall);
					if (mci === earlier_mci) return onDone();
					if (srows.length === 0) // no skiplist
						return addBall(mci - 1);
					var next_mci = mci - 1;
					for (var i = 0; i < srows.length; i++) {
						var next_skiplist_mci = srows[i].main_chain_index;
						if (next_skiplist_mci < next_mci && next_skiplist_mci >= earlier_mci) next_mci = next_skiplist_mci;
					}
					addBall(next_mci);
				});
			});
		});
	}

	if (earlier_mci > later_mci) throw Error("earlier > later");
	if (earlier_mci === later_mci) return onDone();
	addBall(later_mci - 1);
}

// unit's MC index is mci, find a path from mci unit to this unit
function buildLastMileOfProofChain(mci, unit, arrBalls, onDone) {
	function addBall(_unit) {
		db.query("SELECT unit, ball, content_hash FROM units JOIN balls USING(unit) WHERE unit=?", [_unit], function (rows) {
			if (rows.length !== 1) throw Error("no unit?");
			var objBall = rows[0];
			if (objBall.content_hash) objBall.is_nonserial = true;
			delete objBall.content_hash;
			db.query("SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=balls.unit WHERE child_unit=? ORDER BY ball", [objBall.unit], function (parent_rows) {
				if (parent_rows.some(function (parent_row) {
					return !parent_row.ball;
				})) throw Error("some parents have no balls");
				if (parent_rows.length > 0) objBall.parent_balls = parent_rows.map(function (parent_row) {
					return parent_row.ball;
				});
				db.query("SELECT ball \n\
						FROM skiplist_units JOIN units ON skiplist_unit=units.unit LEFT JOIN balls ON units.unit=balls.unit \n\
						WHERE skiplist_units.unit=? ORDER BY ball", [objBall.unit], function (srows) {
					if (srows.some(function (srow) {
						return !srow.ball;
					})) throw Error("last mile: some skiplist units have no balls");
					if (srows.length > 0) objBall.skiplist_balls = srows.map(function (srow) {
						return srow.ball;
					});
					arrBalls.push(objBall);
					if (_unit === unit) return onDone();
					findParent(_unit);
				});
			});
		});
	}

	function findParent(interim_unit) {
		db.query("SELECT parent_unit FROM parenthoods JOIN units ON parent_unit=unit WHERE child_unit=? AND main_chain_index=?", [interim_unit, mci], function (parent_rows) {
			var arrParents = parent_rows.map(function (parent_row) {
				return parent_row.parent_unit;
			});
			if (arrParents.indexOf(unit) >= 0) return addBall(unit);
			async.eachSeries(arrParents, function (parent_unit, cb) {
				graph.determineIfIncluded(db, unit, [parent_unit], function (bIncluded) {
					bIncluded ? cb(parent_unit) : cb();
				});
			}, function (parent_unit) {
				if (!parent_unit) throw Error("no parent that includes target unit");
				addBall(parent_unit);
			});
		});
	}

	// start from MC unit and go back in history
	db.query("SELECT unit FROM units WHERE main_chain_index=? AND is_on_main_chain=1", [mci], function (rows) {
		if (rows.length !== 1) throw Error("no mc unit?");
		var mc_unit = rows[0].unit;
		if (mc_unit === unit) return onDone();
		findParent(mc_unit);
	});
}

function prepareHistory(historyRequest, callbacks) {
	if (!historyRequest) return callbacks.ifError("no history request");
	var arrKnownStableUnits = historyRequest.known_stable_units;
	var arrWitnesses = historyRequest.witnesses;
	var arrAddresses = historyRequest.addresses;
	var arrRequestedJoints = historyRequest.requested_joints;

	if (!arrAddresses && !arrRequestedJoints) return callbacks.ifError("neither addresses nor joints requested");
	if (arrAddresses) {
		if (!ValidationUtils.isNonemptyArray(arrAddresses)) return callbacks.ifError("no addresses");
		if (arrKnownStableUnits && !ValidationUtils.isNonemptyArray(arrKnownStableUnits)) return callbacks.ifError("known_stable_units must be non-empty array");
	}
	if (arrRequestedJoints && !ValidationUtils.isNonemptyArray(arrRequestedJoints)) return callbacks.ifError("no requested joints");
	if (!ValidationUtils.isArrayOfLength(arrWitnesses, constants.COUNT_WITNESSES)) return callbacks.ifError("wrong number of witnesses");

	var assocKnownStableUnits = {};
	if (arrKnownStableUnits) arrKnownStableUnits.forEach(function (unit) {
		assocKnownStableUnits[unit] = true;
	});

	var objResponse = {};

	// add my joints and proofchain to these joints
	var arrSelects = [];
	if (arrAddresses) {
		// we don't filter sequence='good' after the unit is stable, so the client will see final doublespends too
		var strAddressList = arrAddresses.map(db.escape).join(', ');
		arrSelects = ["SELECT DISTINCT unit, main_chain_index, level FROM outputs JOIN units USING(unit) \n\
			WHERE address IN(" + strAddressList + ") AND (+sequence='good' OR is_stable=1) \n\
			UNION \n\
			SELECT DISTINCT unit, main_chain_index, level FROM unit_authors JOIN units USING(unit) \n\
			WHERE address IN(" + strAddressList + ") AND (+sequence='good' OR is_stable=1) \n"];
	}
	if (arrRequestedJoints) {
		var strUnitList = arrRequestedJoints.map(db.escape).join(', ');
		arrSelects.push("SELECT unit, main_chain_index, level FROM units WHERE unit IN(" + strUnitList + ") AND (+sequence='good' OR is_stable=1) \n");
	}
	var sql = arrSelects.join("UNION \n") + "ORDER BY main_chain_index DESC, level DESC";
	db.query(sql, function (rows) {
		// if no matching units, don't build witness proofs
		rows = rows.filter(function (row) {
			return !assocKnownStableUnits[row.unit];
		});
		if (rows.length === 0) return callbacks.ifOk(objResponse);
		if (rows.length > MAX_HISTORY_ITEMS) return callbacks.ifError("your history is too large, consider switching to a full client");

		mutex.lock(['prepareHistory'], function (unlock) {
			var start_ts = Date.now();
			// witnessProof.prepareWitnessProof(
			// 	arrWitnesses, 0,
			// 	function (err, arrUnstableMcJoints, arrWitnessChangeAndDefinitionJoints, last_ball_unit, last_ball_mci) {
			// 		if (err) {
			// 			callbacks.ifError(err);
			// 			return unlock();
			// 		}
			// 		objResponse.unstable_mc_joints = arrUnstableMcJoints;
			// 		if (arrWitnessChangeAndDefinitionJoints.length > 0)
			// 			objResponse.witness_change_and_definition_joints = arrWitnessChangeAndDefinitionJoints;
			//
			// 		// add my joints and proofchain to those joints
			// 		objResponse.joints = [];
			// 		objResponse.proofchain_balls = [];
			// 		var later_mci = last_ball_mci + 1; // +1 so that last ball itself is included in the chain
			// 		async.eachSeries(
			// 			rows,
			// 			function (row, cb2) {
			// 				storage.readJoint(db, row.unit, {
			// 					ifNotFound: function () {
			// 						throw Error("prepareJointsWithProofs unit not found " + row.unit);
			// 					},
			// 					ifFound: function (objJoint) {
			// 						objResponse.joints.push(objJoint);
			// 						if (row.main_chain_index > last_ball_mci || row.main_chain_index === null) // unconfirmed, no proofchain
			// 							return cb2();
			// 						buildProofChain(later_mci, row.main_chain_index, row.unit, objResponse.proofchain_balls, function () {
			// 							later_mci = row.main_chain_index;
			// 							cb2();
			// 						});
			// 					}
			// 				});
			// 			},
			// 			function () {
			// 				//if (objResponse.joints.length > 0 && objResponse.proofchain_balls.length === 0)
			// 				//    throw "no proofs";
			// 				if (objResponse.proofchain_balls.length === 0)
			// 					delete objResponse.proofchain_balls;
			// 				callbacks.ifOk(objResponse);
			// 				console.log("prepareHistory for addresses " + (arrAddresses || []).join(', ') + " and joints " + (arrRequestedJoints || []).join(', ') + " took " + (Date.now() - start_ts) + 'ms');
			// 				unlock();
			// 			}
			// 		);
			// 	}
			// );
		});
	});
}

function processHistory(objResponse, callbacks) {
	if (!("joints" in objResponse)) // nothing found
		return callbacks.ifOk(false);
	if (!ValidationUtils.isNonemptyArray(objResponse.unstable_mc_joints)) return callbacks.ifError("no unstable_mc_joints");
	if (!objResponse.witness_change_and_definition_joints) objResponse.witness_change_and_definition_joints = [];
	if (!Array.isArray(objResponse.witness_change_and_definition_joints)) return callbacks.ifError("witness_change_and_definition_joints must be array");
	if (!ValidationUtils.isNonemptyArray(objResponse.joints)) return callbacks.ifError("no joints");
	if (!objResponse.proofchain_balls) objResponse.proofchain_balls = [];

	// witnessProof.processWitnessProof(
	// 	objResponse.unstable_mc_joints, objResponse.witness_change_and_definition_joints, false,
	// 	function (err, arrLastBallUnits, assocLastBallByLastBallUnit) {
	//
	// 		if (err)
	// 			return callbacks.ifError(err);
	//
	// 		var assocKnownBalls = {};
	// 		for (var unit in assocLastBallByLastBallUnit) {
	// 			var ball = assocLastBallByLastBallUnit[unit];
	// 			assocKnownBalls[ball] = true;
	// 		}
	//
	// 		// proofchain
	// 		var assocProvenUnitsNonserialness = {};
	// 		for (var i = 0; i < objResponse.proofchain_balls.length; i++) {
	// 			var objBall = objResponse.proofchain_balls[i];
	// 			if (objBall.ball !== objectHash.getBallHash(objBall.unit, objBall.parent_balls, objBall.skiplist_balls, objBall.is_nonserial))
	// 				return callbacks.ifError("wrong ball hash: unit " + objBall.unit + ", ball " + objBall.ball);
	// 			if (!assocKnownBalls[objBall.ball])
	// 				return callbacks.ifError("ball not known: " + objBall.ball);
	// 			objBall.parent_balls.forEach(function (parent_ball) {
	// 				assocKnownBalls[parent_ball] = true;
	// 			});
	// 			if (objBall.skiplist_balls)
	// 				objBall.skiplist_balls.forEach(function (skiplist_ball) {
	// 					assocKnownBalls[skiplist_ball] = true;
	// 				});
	// 			assocProvenUnitsNonserialness[objBall.unit] = objBall.is_nonserial;
	// 		}
	// 		assocKnownBalls = null; // free memory
	//
	// 		// joints that pay to/from me and joints that I explicitly requested
	// 		for (var i = 0; i < objResponse.joints.length; i++) {
	// 			var objJoint = objResponse.joints[i];
	// 			var objUnit = objJoint.unit;
	// 			//if (!objJoint.ball)
	// 			//    return callbacks.ifError("stable but no ball");
	// 			if (!validation.hasValidHashes(objJoint))
	// 				return callbacks.ifError("invalid hash");
	// 			if (!ValidationUtils.isPositiveInteger(objUnit.timestamp))
	// 				return callbacks.ifError("no timestamp");
	// 			// we receive unconfirmed units too
	// 			//if (!assocProvenUnitsNonserialness[objUnit.unit])
	// 			//    return callbacks.ifError("proofchain doesn't prove unit "+objUnit.unit);
	// 		}
	//
	// 		// save joints that pay to/from me and joints that I explicitly requested
	// 		mutex.lock(["light_joints"], function (unlock) {
	// 			var arrUnits = objResponse.joints.map(function (objJoint) { return objJoint.unit.unit; });
	// 			breadcrumbs.add('got light_joints for processHistory ' + arrUnits.join(', '));
	// 			db.query("SELECT unit, is_stable FROM units WHERE unit IN(" + arrUnits.map(db.escape).join(', ') + ")", function (rows) {
	// 				var assocExistingUnits = {};
	// 				rows.forEach(function (row) {
	// 					assocExistingUnits[row.unit] = true;
	// 				});
	// 				var arrProvenUnits = [];
	// 				async.eachSeries(
	// 					objResponse.joints.reverse(), // have them in forward chronological order so that we correctly mark is_spent flag
	// 					function (objJoint, cb2) {
	// 						var objUnit = objJoint.unit;
	// 						var unit = objUnit.unit;
	// 						// assocProvenUnitsNonserialness[unit] is true for non-serials, false for serials, undefined for unstable
	// 						var sequence = assocProvenUnitsNonserialness[unit] ? 'final-bad' : 'good';
	// 						if (unit in assocProvenUnitsNonserialness)
	// 							arrProvenUnits.push(unit);
	// 						if (assocExistingUnits[unit]) {
	// 							//if (!assocProvenUnitsNonserialness[objUnit.unit]) // not stable yet
	// 							//    return cb2();
	// 							// it can be null!
	// 							//if (!ValidationUtils.isNonnegativeInteger(objUnit.main_chain_index))
	// 							//    return cb2("bad main_chain_index in proven unit");
	// 							db.query(
	// 								"UPDATE units SET main_chain_index=?, sequence=? WHERE unit=?",
	// 								[objUnit.main_chain_index, sequence, unit],
	// 								function () {
	// 									cb2();
	// 								}
	// 							);
	// 						}
	// 						else
	// 							writer.saveJoint(objJoint, { sequence: sequence, arrDoubleSpendInputs: [], arrAdditionalQueries: [] }, null, cb2);
	// 					},
	// 					function (err) {
	// 						breadcrumbs.add('processHistory almost done');
	// 						if (err) {
	// 							unlock();
	// 							return callbacks.ifError(err);
	// 						}
	// 						fixIsSpentFlagAndInputAddress(function () {
	// 							if (arrProvenUnits.length === 0) {
	// 								unlock();
	// 								return callbacks.ifOk(true);
	// 							}
	// 							db.query("UPDATE units SET is_stable=1, is_free=0 WHERE unit IN(?)", [arrProvenUnits], function () {
	// 								unlock();
	// 								arrProvenUnits = arrProvenUnits.filter(function (unit) { return !assocProvenUnitsNonserialness[unit]; });
	// 								if (arrProvenUnits.length === 0)
	// 									return callbacks.ifOk(true);
	// 								emitStability(arrProvenUnits, function (bEmitted) {
	// 									callbacks.ifOk(!bEmitted);
	// 								});
	// 							});
	// 						});
	// 					}
	// 				);
	// 			});
	// 		});
	//
	// 	}
	// );
}

function fixIsSpentFlag(onDone) {
	db.query("SELECT outputs.unit, outputs.message_index, outputs.output_index \n\
		FROM outputs \n\
		JOIN inputs ON outputs.unit=inputs.src_unit AND outputs.message_index=inputs.src_message_index AND outputs.output_index=inputs.src_output_index \n\
		WHERE is_spent=0 AND type='transfer'", function (rows) {
		console.log(rows.length + " previous outputs appear to be spent");
		if (rows.length === 0) return onDone();
		var arrQueries = [];
		rows.forEach(function (row) {
			console.log('fixing is_spent for output', row);
			db.addQuery(arrQueries, "UPDATE outputs SET is_spent=1 WHERE unit=? AND message_index=? AND output_index=?", [row.unit, row.message_index, row.output_index]);
		});
		async.series(arrQueries, onDone);
	});
}

function fixInputAddress(onDone) {
	db.query("SELECT outputs.unit, outputs.message_index, outputs.output_index, outputs.address \n\
		FROM outputs \n\
		JOIN inputs ON outputs.unit=inputs.src_unit AND outputs.message_index=inputs.src_message_index AND outputs.output_index=inputs.src_output_index \n\
		WHERE inputs.address IS NULL AND type='transfer'", function (rows) {
		console.log(rows.length + " previous inputs appear to be without address");
		if (rows.length === 0) return onDone();
		var arrQueries = [];
		rows.forEach(function (row) {
			console.log('fixing input address for output', row);
			db.addQuery(arrQueries, "UPDATE inputs SET address=? WHERE src_unit=? AND src_message_index=? AND src_output_index=?", [row.address, row.unit, row.message_index, row.output_index]);
		});
		async.series(arrQueries, onDone);
	});
}

function fixIsSpentFlagAndInputAddress(onDone) {
	fixIsSpentFlag(function () {
		fixInputAddress(onDone);
	});
}

function determineIfHaveUnstableJoints(arrAddresses, handleResult) {
	if (arrAddresses.length === 0) return handleResult(false);
	db.query("SELECT DISTINCT unit, main_chain_index FROM outputs JOIN units USING(unit) \n\
		WHERE address IN(?) AND +sequence='good' AND is_stable=0 \n\
		UNION \n\
		SELECT DISTINCT unit, main_chain_index FROM unit_authors JOIN units USING(unit) \n\
		WHERE address IN(?) AND +sequence='good' AND is_stable=0 \n\
		LIMIT 1", [arrAddresses, arrAddresses], function (rows) {
		handleResult(rows.length > 0);
	});
}

function emitStability(arrProvenUnits, onDone) {
	var strUnitList = arrProvenUnits.map(db.escape).join(', ');
	db.query("SELECT unit FROM unit_authors JOIN my_addresses USING(address) WHERE unit IN(" + strUnitList + ") \n\
		UNION \n\
		SELECT unit FROM outputs JOIN my_addresses USING(address) WHERE unit IN(" + strUnitList + ") \n\
		UNION \n\
		SELECT unit FROM unit_authors JOIN shared_addresses ON address=shared_address WHERE unit IN(" + strUnitList + ") \n\
		UNION \n\
		SELECT unit FROM outputs JOIN shared_addresses ON address=shared_address WHERE unit IN(" + strUnitList + ")", function (rows) {
		onDone(rows.length > 0);
		if (rows.length > 0) {
			eventBus.emit('my_transactions_became_stable', rows.map(function (row) {
				return row.unit;
			}));
			rows.forEach(function (row) {
				eventBus.emit('my_stable-' + row.unit);
			});
		}
	});
}

function prepareParentsAndLastBallAndWitnessListUnit(arrWitnesses, callbacks) {
	if (!ValidationUtils.isArrayOfLength(arrWitnesses, constants.COUNT_WITNESSES)) return callbacks.ifError("wrong number of witnesses");
	storage.determineIfWitnessAddressDefinitionsHaveReferences(db, arrWitnesses, function (bWithReferences) {
		if (bWithReferences) return callbacks.ifError("some witnesses have references in their addresses");
		parentComposer.pickParentUnitsAndLastBall(db, arrWitnesses, function (err, arrParentUnits, last_stable_mc_ball, last_stable_mc_ball_unit, last_stable_mc_ball_mci) {
			if (err) return callbacks.ifError("unable to find parents: " + err);
			var objResponse = {
				parent_units: arrParentUnits,
				last_stable_mc_ball: last_stable_mc_ball,
				last_stable_mc_ball_unit: last_stable_mc_ball_unit,
				last_stable_mc_ball_mci: last_stable_mc_ball_mci
			};
			storage.findWitnessListUnit(db, arrWitnesses, last_stable_mc_ball_mci, function (witness_list_unit) {
				if (witness_list_unit) objResponse.witness_list_unit = witness_list_unit;
				callbacks.ifOk(objResponse);
			});
		});
	});
}

// arrUnits sorted in reverse chronological order
function prepareLinkProofs(arrUnits, callbacks) {
	if (!ValidationUtils.isNonemptyArray(arrUnits)) return callbacks.ifError("no units array");
	if (arrUnits.length === 1) return callbacks.ifError("chain of one element");
	mutex.lock(['prepareLinkProofs'], function (unlock) {
		var start_ts = Date.now();
		var arrChain = [];
		async.forEachOfSeries(arrUnits, function (unit, i, cb) {
			if (i === 0) return cb();
			createLinkProof(arrUnits[i - 1], arrUnits[i], arrChain, cb);
		}, function (err) {
			console.log("prepareLinkProofs for units " + arrUnits.join(', ') + " took " + (Date.now() - start_ts) + 'ms, err=' + err);
			err ? callbacks.ifError(err) : callbacks.ifOk(arrChain);
			unlock();
		});
	});
}

// adds later unit
// earlier unit is not included in the chain
function createLinkProof(later_unit, earlier_unit, arrChain, cb) {
	storage.readJoint(db, later_unit, {
		ifNotFound: function ifNotFound() {
			cb("later unit not found");
		},
		ifFound: function ifFound(objLaterJoint) {
			var later_mci = objLaterJoint.unit.main_chain_index;
			arrChain.push(objLaterJoint);
			storage.readUnitProps(db, objLaterJoint.unit.last_ball_unit, function (objLaterLastBallUnitProps) {
				var later_lb_mci = objLaterLastBallUnitProps.main_chain_index;
				storage.readJoint(db, earlier_unit, {
					ifNotFound: function ifNotFound() {
						cb("earlier unit not found");
					},
					ifFound: function ifFound(objEarlierJoint) {
						var earlier_mci = objEarlierJoint.unit.main_chain_index;
						var earlier_unit = objEarlierJoint.unit.unit;
						if (later_mci < earlier_mci) return cb("not included");
						if (later_lb_mci >= earlier_mci) {
							// was spent when confirmed
							// includes the ball of earlier unit
							buildProofChain(later_lb_mci + 1, earlier_mci, earlier_unit, arrChain, function () {
								cb();
							});
						} else {
							// the output was unconfirmed when spent
							graph.determineIfIncluded(db, earlier_unit, [later_unit], function (bIncluded) {
								if (!bIncluded) return cb("not included");
								buildPath(objLaterJoint, objEarlierJoint, arrChain, function () {
									cb();
								});
							});
						}
					}
				});
			});
		}
	});
}

// build parent path from later unit to earlier unit and add all joints along the path into arrChain
// arrChain will include later unit but not include earlier unit
// assuming arrChain already includes later unit
function buildPath(objLaterJoint, objEarlierJoint, arrChain, onDone) {

	function addJoint(unit, onAdded) {
		storage.readJoint(db, unit, {
			ifNotFound: function ifNotFound() {
				throw Error("unit not found?");
			},
			ifFound: function ifFound(objJoint) {
				arrChain.push(objJoint);
				onAdded(objJoint);
			}
		});
	}

	function goUp(objChildJoint) {
		db.query("SELECT parent.unit, parent.main_chain_index FROM units AS child JOIN units AS parent ON child.best_parent_unit=parent.unit \n\
			WHERE child.unit=?", [objChildJoint.unit.unit], function (rows) {
			if (rows.length !== 1) throw Error("goUp not 1 parent");
			if (rows[0].main_chain_index < objEarlierJoint.unit.main_chain_index) // jumped over the target
				return buildPathToEarlierUnit(objChildJoint);
			addJoint(rows[0].unit, function (objJoint) {
				objJoint.unit.main_chain_index === objEarlierJoint.unit.main_chain_index ? buildPathToEarlierUnit(objJoint) : goUp(objJoint);
			});
		});
	}

	function buildPathToEarlierUnit(objJoint) {
		db.query("SELECT unit FROM parenthoods JOIN units ON parent_unit=unit \n\
			WHERE child_unit=? AND main_chain_index=?", [objJoint.unit.unit, objJoint.unit.main_chain_index], function (rows) {
			if (rows.length === 0) throw Error("no parents with same mci?");
			var arrParentUnits = rows.map(function (row) {
				return row.unit;
			});
			if (arrParentUnits.indexOf(objEarlierJoint.unit.unit) >= 0) return onDone();
			if (arrParentUnits.length === 1) return addJoint(arrParentUnits[0], buildPathToEarlierUnit);
			// find any parent that includes earlier unit
			async.eachSeries(arrParentUnits, function (unit, cb) {
				graph.determineIfIncluded(db, objEarlierJoint.unit.unit, [unit], function (bIncluded) {
					if (!bIncluded) return cb(); // try next
					cb(unit); // abort the eachSeries
				});
			}, function (unit) {
				if (!unit) throw Error("none of the parents includes earlier unit");
				addJoint(unit, buildPathToEarlierUnit);
			});
		});
	}

	if (objLaterJoint.unit.unit === objEarlierJoint.unit.unit) return onDone();
	objLaterJoint.unit.main_chain_index === objEarlierJoint.unit.main_chain_index ? buildPathToEarlierUnit(objLaterJoint) : goUp(objLaterJoint);
}

function processLinkProofs(arrUnits, arrChain, callbacks) {
	// check first element
	var objFirstJoint = arrChain[0];
	if (!objFirstJoint || !objFirstJoint.unit || objFirstJoint.unit.unit !== arrUnits[0]) return callbacks.ifError("unexpected 1st element");
	var assocKnownUnits = {};
	var assocKnownBalls = {};
	assocKnownUnits[arrUnits[0]] = true;
	for (var i = 0; i < arrChain.length; i++) {
		var objElement = arrChain[i];
		if (objElement.unit && objElement.unit.unit) {
			var objJoint = objElement;
			var objUnit = objJoint.unit;
			var unit = objUnit.unit;
			if (!assocKnownUnits[unit]) return callbacks.ifError("unknown unit " + unit);
			if (!validation.hasValidHashes(objJoint)) return callbacks.ifError("invalid hash of unit " + unit);
			assocKnownBalls[objUnit.last_ball] = true;
			assocKnownUnits[objUnit.last_ball_unit] = true;
			objUnit.parent_units.forEach(function (parent_unit) {
				assocKnownUnits[parent_unit] = true;
			});
		} else if (objElement.unit && objElement.ball) {
			var objBall = objElement;
			if (!assocKnownBalls[objBall.ball]) return callbacks.ifError("unknown ball " + objBall.ball);
			if (objBall.ball !== objectHash.getBallHash(objBall.unit, objBall.parent_balls, objBall.skiplist_balls, objBall.is_nonserial)) return callbacks.ifError("invalid ball hash");
			objBall.parent_balls.forEach(function (parent_ball) {
				assocKnownBalls[parent_ball] = true;
			});
			if (objBall.skiplist_balls) objBall.skiplist_balls.forEach(function (skiplist_ball) {
				assocKnownBalls[skiplist_ball] = true;
			});
			assocKnownUnits[objBall.unit] = true;
		} else return callbacks.ifError("unrecognized chain element");
	}
	// so, the chain is valid, now check that we can find the requested units in the chain
	for (var i = 1; i < arrUnits.length; i++) {
		// skipped first unit which was already checked
		if (!assocKnownUnits[arrUnits[i]]) return callbacks.ifError("unit " + arrUnits[i] + " not found in the chain");
	}callbacks.ifOk();
}

exports.prepareHistory = prepareHistory;
exports.processHistory = processHistory;
exports.prepareLinkProofs = prepareLinkProofs;
exports.processLinkProofs = processLinkProofs;
exports.determineIfHaveUnstableJoints = determineIfHaveUnstableJoints;
exports.prepareParentsAndLastBallAndWitnessListUnit = prepareParentsAndLastBallAndWitnessListUnit;
exports.updateHistory = updateHistory;
exports.insertHistory = insertHistory;