/**
 * Mail-folders-only IMAProxy module
 *
 * Intercepts LSUB and LIST responses and removes non-mail folders
 * from the listing after checking the /vendor/kolab/folder-type annotations.
 *
 * @author Thomas Bruederli <thomas@roundcube.net>
 *
 * Copyright (C) 2014, Thomas Bruederli, Bern, Switzerland
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

"use strict";

var imap = require('../lib/imap.js');

/**
 * Mail-folders-only IMAProxy plugin
 */
function Mailonly(proxy)
{
    var TYPE_ANNOTATION = "/vendor/kolab/folder-type";
    var listening = false;
    var capabilities = {};
    var metadata = [];
    var proc = [];

    // public methods
    this.init = init;

    /**
     * Plugin init method
     */
    function init()
    {
        proxy.clientEmitter.on('LSUB', clientList);
        proxy.clientEmitter.on('LIST', clientList);
        proxy.clientEmitter.on('XLIST', clientList);
        proxy.clientEmitter.on('__DISCONNECT__', clientDisconnect);
        proxy.serverEmitter.on('OK', OKResponse);
        proxy.serverEmitter.once('CAPABILITY', capabilityResponse);
    }

    /**
     * Handle OK responses which might have Capabilities appended
     */
    function OKResponse(event, data)
    {
        var response = imap.parseResponse(data);
        if (response.lines[0].match(/\[CAPABILITY\s/)) {
            parseCapabilities(response.lines[0].replace(/\sOK/, '').replace(/\[|\]/i, ''));
            if (capabilities['SORT'] || capabilities['ANNOTATEMORE'] || capabilities['METADATA']) {
                proxy.serverEmitter.removeListener('OK', OKResponse);
            }
        }
    }

    /**
     * Handle CAPABILITY response
     */
    function capabilityResponse(event, data)
    {
        var response = imap.parseResponse(data);

        if (response.status === 'OK') {
            parseCapabilities(response.lines[0]);
            proxy.serverEmitter.removeListener('OK', OKResponse);
        }
    }

    /**
     * Parse IMAP server capabilities
     */
    function parseCapabilities(line)
    {
        var c, i, caps = imap.explodeQuotedString(line, " ");
        for (i=2; i < caps.length; i++) {
            c = caps[i].split('=');
            capabilities[c[0]] = c[1] || true;
        }
    }

    /**
     * Handler for client LSUB or LIST commands
     */
    function clientList(event, data)
    {
        // nothing to do here
        if (!capabilities['ANNOTATEMORE'] && !capabilities['METADATA']) {
            proxy.clientEmitter.removeListener('LSUB', clientList);
            proxy.clientEmitter.removeListener('LIST', clientList);
            proxy.clientEmitter.removeListener('XLIST', clientList);
            proxy.clientEmitter.removeListener('__DISCONNECT__', clientDisconnect);
            return;
        }

        // register new LSUB/LIST/XLIST request for this connection
        if (!proc[event.state.ID]) {
            proc[event.state.ID] = { buffer:'', listings:{}, pending:0 };
        }

        var i, req, listing, lines = data.toString().trim().split(/\r?\n/);
        for (i=0; i < lines.length; i++) {
            req = imap.tokenizeData(lines[i], 2);
            listing = { seq: req[0], command: req[1], buffer: [] };

            proc[event.state.ID].listings['A' + listing.seq] = listing;
            proc[event.state.ID].pending++;
        }

        // listen to server responses
        if (!listening) {
            proxy.serverEmitter.on('__DATA__', serverResponse);
            listening = true;
        }
    }

    /**
     * Handler for server responses
     */
    function serverResponse(event, data)
    {
        var req, last, response, id = event.state.ID;

        // buffering is active for this connection
        if (req = proc[id]) {
            event.write = false;  // don't forward to client

            response = imap.parseResponse(data);
            last = response.lines.pop();

            // GETANNOTATION completed
            if (response.seq && req.listings[response.seq] && capabilities['ANNOTATEMORE']) {
                var i, ann, values, lines = (req.buffer + data.toString()).trim().split(/\r?\n/);
                for (i=0; i < lines.length; i++) {
                    ann = imap.tokenizeData(lines[i], 5);
                    values = ann[4] || [];

                    if (metadata[id] === undefined) {
                        metadata[id] = {};
                    }

                    // store folder type in global (per-connection) memory for subsequent requests (e.g. XLIST + LSUB)
                    if (ann[1] === 'ANNOTATION' && ann[3] === TYPE_ANNOTATION && values.length) {
                        metadata[id][ann[2]] = (values[1] || values[3] || '').replace(/\..+$/, '');
                    }
                }

                // clear buffer
                req.buffer = '';

                // filter buffered listing and send it to client
                sendFilteredList(id, response.seq, event);
            }
            // GETMETADATA completed
            else if (response.seq && req.listings[response.seq] && capabilities['METADATA']) {
                var MD_PREFIX = '* METADATA ';
                var i, j, lines = (req.buffer + data.toString()).trim().split(/\r?\n/);

//              console.log('BEG PROCESS METADATA');
//              console.log(lines.join('\n'));
//              console.log('END PROCESS METADATA');

                for (i=0; i < lines.length; i++) {
                    var line = lines[i];

                    // Skip non metadata lines
                    if (line.substring(0, MD_PREFIX.length) !== MD_PREFIX)
                        continue;

                    // folder, entries and values
                    var folder, entry, entries = [];

                    // token related variables
                    var tbeg, tend, tsep, eol;

                    // First find the foldername
                    folder = '';
                    tbeg = MD_PREFIX.length;

                    // No folder name, ignore the line
                    if (tbeg >= line.length) continue;

                    // See if the name is quoted
                    if (line[tbeg] === '"') {
                        tsep = '"'; tbeg++;
                    } else {
                        tsep = ' ';
                    }
                    tend = tbeg;
                    while (tend < line.length) {
                        if (line[tend] === '\\') {
                            tend++;
                        } else if (line[tend] === tsep) {
                            folder = line.substring(tbeg, tend);
//                          console.log("Found folder '" + folder + "'");
                            tbeg = tend + 1;
                            break;
                        }
                        tend++;
                    }

                    // Move to the beginning of the entry definitions
                    if (tsep === '"') tbeg++;

                    // No entry name, ignore the line
                    if (tbeg >= line.length) continue;

                    if (line[tbeg] === '(') {
                        eol = ')'; tbeg++;
                        if (tbeg >= line.length) continue;
                    } else {
                        eol = '';
                    }

                    // Find entry names and values
                    tend = tbeg;
                    while (tend < line.length && line[tend] !== eol) {
                        while (tend < line.length) {
                            if (line[tend] === ' ') {
                                entry = line.substring(tbeg, tend);
//                              console.log("Found entry '" + entry + "'");
                                tend++;
                                tbeg = tend;
                                break;
                            }
                            tend++;
                        }

                        // No entry value, ignore the line
                        if (tbeg >= line.length) break;

                        // Value can be a string or an integer between { and }
                        tend = tbeg;
//                      console.log('line[tend (' + tend + ')] = ' + line[tend]);
                        if (line[tend] === '{') {
                            tend++;
                            tbeg = tend;
                            while (tend < line.length) {
                                if (line[tend] === '}') {
                                    var vlen = parseInt(line.substring(tbeg, tend));
//                                  console.log('vlen string = ' + line.substring(tbeg, tend) + '; vlen = ' + vlen);
                                    tend++;
                                    tbeg = tend;
                                    if (tend === line.length) {
                                        i++;
                                        // Wrong value, skip line again
                                        if (i >= lines.length) break;
                                        // get the value from the next line
                                        line = lines[i];
                                        tbeg = 0;
                                        tend = vlen;
                                    } else {
                                        tend = tbeg + vlen;
                                    }
//                                  console.log("Entry '" + entry + "' value = '" + line.substring(tbeg,tend) + "'");
                                    entries.push([ entry, line.substring(tbeg, tend) ]);
                                    tend++;
                                    tbeg = tend;
                                    break;
                                }
                                tend++;
                            }
                        } else {
                           tbeg = tend;
                           while (tend < line.length) {
                               if (line[tend] === ' ' || line[tend] === eol) {
//                                  console.log("Entry '" + entry + "' value = '" + line.substring(tbeg,tend) + "'");
                                    entries.push([ entry, line.substring(tbeg, tend) ]);
                                    tend++;
                                    tbeg = tend;
                                    break;
                               }
                               tend++;
                           }
                        }
                    }
//                  console.log("ENTRIES = '" + entries + "'");
                    if (metadata[id] === undefined) {
                        metadata[id] = {};
                    }
                    for (j=0; j < entries.length; j++) {
                        if (entries[j][0] === '/private' + TYPE_ANNOTATION
						    || entries[j][0] === '/shared' + TYPE_ANNOTATION) {
                            if (entries[j][1] === 'NIL') {
                                metadata[id][folder] = 'NIL';
                            } else {
                                metadata[id][folder] = entries[j][1].replace(/\..+$/, '');
//                              console.log("Folder '" + folder + "' metadata = '" + metadata[id][folder] + "'");
                                break;
                            }
                        }
                    }
                }

                // clear buffer
                req.buffer = '';

                // filter buffered listing and send it to client
                sendFilteredList(id, response.seq, event);
            }
            else {
                req.buffer += data.toString();

                // command done
                if (response.seq) {
                    // pipe through unrelated results
                    event.write = !processListing(id, response.seq, req.buffer, event);

                    // send all buffered data to client
                    if (event.write && req.buffer) {
                        event.result = req.buffer;
                    }

                    // clear buffer
                    req.buffer = '';
                }
            }
        }
    }

    /**
     * Process the collected server response on a listing command
     */
    function processListing(id, seq, buffer, event)
    {
        var i, req = proc[id], listing = req.listings['A' + seq],
            lines = buffer.trim().split(/\r?\n/);

        // tag doesn't match an active listing command or response is empty
        if (!listing || lines.length < 2) {
            listingDone(id, 'A'+seq);
            return false;
        }

        // remove response line
        lines.pop();

        // get metadata for every mailbox name
        for (i=0; i < lines.length; i++) {
            listing.buffer.push(lines[i]);
        }

        // we already collected all annotations, send the (filtered) response to the client
        if (metadata[id] !== undefined && metadata[id] !== {}) {
            sendFilteredList(id, 'A' + seq, event);
        }
        else if (capabilities['ANNOTATEMORE']) {
            // fetch all folder annotations in one go
            metadata[id] = {};
            event.server.write('A' + seq + ' GETANNOTATION "*" "' + TYPE_ANNOTATION + '" ("value.priv" "value.shared")\r\n');
        }
        else {
            // fetch all folder metadata in one go
            metadata[id] = {};
            event.server.write('A' + seq + ' GETMETADATA "*" (/private' + TYPE_ANNOTATION + ' /shared' + TYPE_ANNOTATION +')\r\n');
        }

        return true;
    }

    /**
     * Filter and send the buffered list for the given sequence tag
     */
    function sendFilteredList(id, seq, event)
    {
        var req, listing;
        if ((req = proc[id]) && (listing = req.listings[seq])) {
            proxy.config.debug_log && console.log("Mailonly filter:", listing.buffer, metadata[id]);

            var i, rec, mbox, type, list = [];
            for (i=0; i < listing.buffer.length; i++) {
                rec = imap.tokenizeData(listing.buffer[i]);
                mbox = rec.pop();
                // Don't show folders under the shared namespace
                if (mbox.match(/^shared($|\/)/)) continue;
                type = metadata[id][mbox];

                if (!type || type === 'mail' || type === 'NIL') {
                    list.push(listing.buffer[i]);
                }
            }

            // send filtered list as response to the client
            event.result = list.join("\r\n")  + "\r\n" +
                listing.seq + " OK Completed (filtered by IMAProxy)\r\n";

            // destroy listing job
            listingDone(id, seq);
        }
    }

    /**
     * Terminate a buffered listing command and remporarily stored data
     */
    function listingDone(id, seq)
    {
        var req;
        if ((req = proc[id]) && req.listings[seq]) {
            // destroy listing job
            delete req.listings[seq];
            req.pending--;

            // all done for this connection, suspend response capturing
            if (req.pending === 0) {
                delete proc[id];
            }
        }
    }

    /**
     * Handler for client disconnect
     */
    function clientDisconnect(event)
    {
        delete proc[event.state.ID];
        delete metadata[event.state.ID];

        // TODO: remove serverEmitter listeners if no further jobs pending
    }

}

module.exports = Mailonly;

