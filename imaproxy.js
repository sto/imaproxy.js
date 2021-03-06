/**
 * "I'M A Proxy" - IMAP proxy server to monitor and modify IMAP communications
 *
 * Plugins can subscribe to certain IMAP commands sent by either the client or the server
 * and alter the exchanged data. The primary use is to hide non-mail folders of a Kolab
 * server by filtering LSUB and LIST responses. See plugins/mailonly.js
 *
 * Inspired by http://www.tobinindustries.com/blog/2013/09/09/inspect-imap-traffic-using-a-nodejs-proxy/
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

var fs = require("fs"),
    tls = require("tls"),
    net = require("net"),
    url = require("url"),
    events = require("events"),
    cluster = require("cluster");

/**
 * IMAP proxy class
 */
function IMAProxy(config)
{
    var PID = '';
    var ID_COUNT    = 0;
    var WHITE_CCODE = '\x1b[0;37m';
    var CONN_LOG    = true;

    var clientEmitter = new events.EventEmitter();
    var serverEmitter = new events.EventEmitter();
    var imap_server = url.parse(config.imap_server);
    var connections = 0;
    var plugins = [];
    var self = this;

    // exports
    this.clientEmitter = clientEmitter;
    this.serverEmitter = serverEmitter;
    this.config = config;

    // public methods
    this.start = start;

    /**
     * Initialize the proxy
     */
    function init()
    {
        // fix/complete imap server config
        if (!imap_server.hostname) {
            imap_server.hostname = imap_server.path;
        }
        if (!imap_server.port) {
            imap_server.port = imap_server.protocol === 'tls:' || imap_server.protocol === 'ssl:' ? 993 : 143;
        }

        PID = cluster.isWorker ? cluster.worker.id + ':' : '';
        CONN_LOG  = config.connection_log || true;

        // remove "DEFLATE" from capabilities (if present) so this proxy doesn't have to decompress stuff
        serverEmitter.on('CAPABILITY', function(event, data){
            var str = data.toString();
            if (str.match(/COMPRESS=DEFLATE/)) {
                event.result = str.replace("COMPRESS=DEFLATE ", "");
            }
            if (str.match(/ (SORT|ANNOTATEMORE)/)) {
                event.state.capabilities = true;
            }
        });
        // ... also intercept for OK [CAPABILITY ...] responses
        serverEmitter.on('OK', function(event, data){
            if (!event.state.capabilities) {
                var str = data.toString();
                if (str.match(/\[CAPABILITY\s/) && str.match(/\s(SORT|ANNOTATEMORE)/)) {
                    if (str.match(/COMPRESS=DEFLATE/)) {
                        event.result = str.replace("COMPRESS=DEFLATE ", "");
                    }
                    event.state.capabilities = true;
                }
            }
        });

        // load modules that register event listeners
        var k, p, plugin, files = fs.readdirSync(__dirname + '/plugins');
        for (k in files) {
            if (!files[k].match(/\.js$/)) {
                continue;
            }

            try {
                plugin = require(__dirname + '/plugins/' + files[k]);
                p = new plugin(self);
                p.init();
                plugins.push(p);
            }
            catch (e) {
                console.error("Failed to load plugin " + files[k], e);
            }
        }
    }

    /**
     * Handler for new connections from mail clients
     */
    function clientListener(connectionToClient)
    {
        connections++;

        // This callback is run when the server gets a connection from a client.
        var connectionToServer, state = { ID: ++ID_COUNT, isConnected: true, capabilities: false }, prefix = "[" + PID + state.ID + "] ", client_buffer = '';
        CONN_LOG && console.log(WHITE_CCODE + prefix + "* Connection established from %s:%d; open connections: %d",
            connectionToClient.remoteAddress, connectionToClient.remotePort, connections);

        // print TLS connection details
        if (CONN_LOG && connectionToClient.getCipher) {
            console.log(prefix + "* Using " + connectionToClient.getCipher().name + "; " + connectionToClient.getCipher().version);
        }

        function extend_event(event) {
            event.server = connectionToServer;
            event.client = connectionToClient;
            event.state = state;
            return event;
        }

        connectionToClient.on("data", function(data) {
            var cmd = parseIMAPCommand(data, client_buffer);

            // buffer short inputs leading to split tags (observed with Apple Mail)
            if (!cmd.write) {
                client_buffer += data.toString();
                return;
            }
            if (client_buffer.length) {
                // concatenate buffered string with current data
                data = Buffer.concat([new Buffer(client_buffer), data]);
                client_buffer = '';
            }

            // emit events with client data
            var event = extend_event(cmd);
            clientEmitter.emit(event.command, event, data);
            if (event.command !== '__DATA__') {
                clientEmitter.emit('__DATA__', event, data);
            }
            clientEmitter.emit('__POSTDATA__', event, data);

            if (event.result) {
                connectionToServer.write(event.result);
            }
            else if (event.write) {
                connectionToServer.write(data);
            }
        });

        connectionToClient.on("error", function(e){
            console.error(WHITE_CCODE + prefix + "* Client connection error!", e);
            if (state.isConnected) {
                connectionToServer.end();
                connections--;
            }
        });

        connectionToClient.on("close", function(){
            CONN_LOG && console.log(WHITE_CCODE + prefix + "* Client connection closed");
            if (state.isConnected) {
                state.isConnected = false;
                connectionToServer.end();
                connections--;
            }
            clientEmitter.emit('__DISCONNECT__', extend_event({}));
        });

        // emit client connection event
        clientEmitter.emit('__CONNECT__', extend_event({}));

        // Now that we have a client on the line, make a connection to the IMAP server.
        state.conn = new net.Socket();

        // establish a SSL/TLS connection
        if (imap_server.protocol === 'tls:' || imap_server.protocol === 'ssl:') {
            connectionToServer = tls.connect({
                    socket: state.conn,
                    rejectUnauthorized: !config.tls_nocheck_certs
                }, function() {
                    CONN_LOG && console.log(WHITE_CCODE + prefix + "* Client connected");
                    state.conn = connectionToServer;
                });
        }
        else {
            connectionToServer = state.conn;
        }

        connectionToServer.on("data", function(data) {
            var str = data.toString();
            if (!state.isConnected) {
                return;
            }

            var cmd = parseIMAPCommand(data, '');
            cmd.write = true;  // always send by default

            // emit events with server data
            var event = extend_event(cmd);
            serverEmitter.emit(event.command, event, data);
            if (event.command !== '__DATA__') {
                serverEmitter.emit('__DATA__', event, data);
            }
            serverEmitter.emit('__POSTDATA__', event, data);

            if (event.result) {
                connectionToClient.write(event.result);
            }
            else if (event.write) {
                connectionToClient.write(data);
            }
        });

        connectionToServer.on("timeout", function(){
            CONN_LOG && console.log(WHITE_CCODE + prefix + "* Server connection timeout!");
            connectionToServer.end();
            connectionToClient.end();
        });

        connectionToServer.on("error", function(e){
            console.error(WHITE_CCODE + prefix + "* Server connection error!", e);
            connectionToServer.destroy();
            connectionToClient.end();
        });

        connectionToServer.on("close", function(){
            CONN_LOG && console.log(WHITE_CCODE + prefix + "* Disconnected from " + imap_server.hostname);
            if (state.isConnected) {
                state.isConnected = false;
                connectionToClient.end();
                connections--;
            }
            serverEmitter.emit('__DISCONNECT__', extend_event({}));
        });

        // connect to IMAP server
        state.conn.connect(imap_server.port, imap_server.hostname, function(){
            var e = extend_event({});
            serverEmitter.emit('__CONNECT__', e);

            if (config.keep_alive) {
                state.conn.setKeepAlive(true, config.keep_alive * 1000);
            }
        });
    }

    /**
     * Create server instace listening on incoming IMAP connections
     */
    function start()
    {
        // use tls for secured connections if configured
        var server;
        if (config.ssl) {
            var options = {
                key:  fs.readFileSync(config.ssl_key),
                cert: fs.readFileSync(config.ssl_cert),
                ca: [ fs.readFileSync(config.ssl_ca) ]
            };
            server = tls.createServer(options, clientListener);
        }
        else {
            server = net.createServer(clientListener);
        }

        server.listen(config.bind_port, function() {
            console.log(WHITE_CCODE + "* IMAP proxy" + (cluster.isWorker ? " (" + cluster.worker.id + ")" : '') + " is listening on port " + config.bind_port);
        });
    }

    /**
     * Simple utility function to parse an IMAP command or response.
     * Extracts the actual command and the sequence number.
     */
    function parseIMAPCommand(data, old_buffer)
    {
        var str = old_buffer + data.toString('utf8', 0, 256),
            lines = str.split(/\r?\n/),
            tokens = String(lines[0]).split(/ +/),
            cmd = { seq: 0, command: '__DATA__', write: true };

        if (tokens.length > 1 && tokens[1].match(/^[a-z]+$/i)) {
            cmd.seq = tokens[0];
            cmd.command = tokens[1].toUpperCase();
        }
        else if (tokens.length === 1 && tokens[0].match(/^[a-z]+$/i)) {
            cmd.command = tokens[0].toUpperCase();
        }
        else if (tokens.length === 1 && lines.length === 1 && str.length < 10) {
            // incomplete tag, don't forward to receiver
            cmd.write = false;
        }

        // UID X command
        if (cmd.command === 'UID') {
            cmd.command += ' ' + String(tokens[2]).toUpperCase();
        }

        return cmd;
    }

    init();
}


/////////////////////////  main()

var configfile = './config.js';

if (process.argv.length > 2) {
    configfile = process.argv[2];
}

var config = require(configfile);

// fork child processes
if (cluster.isMaster && config.workers) {
  for (var i = 0; i < config.workers; i++) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {
    if (code !== 0) {
        // restart a crashed child process
        console.warn("* Worker %d (PID=%s) died with code %d. Restarting...", worker.id, worker.process.pid, code);
        cluster.fork();
    }
    else {
        console.log("Worker %d (PID=%s) exited with signal %s", worker.id, worker.process.pid, signal);
    }
  });
}
else {
    var proxy = new IMAProxy(config);
    proxy.start();
}

