/**
 * @module CassandraDBConnector
 * @description Cassandra Client
 */

var config = require('config'),
    dbConfig = config.get('cassandra.default'),
    driver = require('cassandra-driver'),
    Logger = require('../bunyan/Bunyan.js'),
    logger = new Logger('NODE', 'Cassandra'),
    _ = require('lodash');

var connections = {};

function Client(){
    function getConnection(hostName){
        hostName = hostName || 'default';

        var configPath = 'cassandra.' + hostName;

        if(!_.has(config, configPath)){
            var msg = 'LIB_CAS_DB_1';
            logger.error(msg);
            throw new Error(msg);
        }
        var host = config.get(configPath).get('host');
        host = Array.isArray(host) ? host : [host];
        var hostKey = host.join('|');

        if(!_.has(connections, hostKey)){
            var client = new driver.Client({
                contactPoints: host
            });
            client.connect(function (err) {
                if (err) {
                    logger.error(err);
                    return;
                }
                logger.info('Connected to Cassandra at ' + hostKey);
            });

            connections[hostKey] = client;
        }
        return connections[hostKey];
    }

    this.getConnection = getConnection;
}

module.exports = Client;