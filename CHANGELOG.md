# Changelog

## [v2.3.0] - 2026-10-11

### Fixed

* fix: re-subscribing (redeploy) replaced nothing: messages went to the closed node. Callbacks are now owned by the node id and replaced
* fix: subscribe node removes its callbacks on close
* fix: a filter covered by an existing one (e.g. `a/b` under `a/+`) was silently ignored
* fix: subscriptions were dropped on every connection `close` (including before a reconnection); they are now re-sent on `connect`
* fix: `reconnect_t` (documented) was ignored, only `reconnect` was read: auto-reconnect never enabled
* fix: connect on an existing reconnecting client replaced it and lost its subscriptions
* fix: topic matching interpreted regex characters (`.`); now level-by-level MQTT matching
* fix: a message is delivered to every matching filter, not only the first

## [v2.2.0] - 2025-02-05

### Added

* feat: support mqtt publish options

## [v2.1.0] - 2024-09-05

### Added

* feat: add mqtt rap & rh options


### Changed

* ref: extract DynMQTT class
* chore: add comments
* chore: improve log
* ref: use async method for publish, subscribe & unsubscribe
* chore: add comments
* chore: improve log

### Fixed

* fix: subsciptions topic declaration


## [v2.0.0] - 2024-09-05


### Changed

* chore: update mqtt version (Removes support for all end of life node versions v12 and v14)

### Fixed

* fix: use destr module instead JSON.parse to be more robust ex: JSON.parse("False") => exception , destr("False") => false and avoid bugs

* fix:  this.status_callback({{ fill: "red", shape: "dot", text: err } }); =>  this.status_callback({ summary: { fill: "red", shape: "dot", text: err } }); (see issue https://github.com/eltonssilva/node-red-contrib-mqtt-auto/issues/1)
