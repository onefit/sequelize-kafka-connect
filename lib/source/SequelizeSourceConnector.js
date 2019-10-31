"use strict";

const { SourceConnector } = require("kafka-connect");
const Sequelize = require("sequelize");
const SequelizeAuto = require("sequelize-auto");

class SequelizeSourceConnector extends SourceConnector {

    start(properties, callback) {

        this.properties = properties;

        this.sequelize = new Sequelize(properties.database,
            properties.user, properties.password,
            properties.options);

        this.sequelizeAuto = new SequelizeAuto(properties.database,
            properties.user, properties.password, Object.assign({},
                properties.options, {
                    additional: {
                        timestamps: false
                    },
                    tables: [properties.table],
                    directory: false //do not write models to fs
                }));

        this.sequelize.authenticate().then(() => {

            this.sequelizeAuto.run(error => {

                if (error) {
                    return callback(error);
                }

                if (!this.sequelizeAuto.tables ||
                    !this.sequelizeAuto.tables[this.properties.table]) {
                    return callback(new Error("Failed to load table schema, its empty."));
                }

                this.tableSchema = this.sequelizeAuto.tables[this.properties.table];
                callback(null);
            });

        }).catch(error => {
            callback(error);
        });
    }

    taskConfigs(maxTasks, callback) {

        const taskConfig = {
            maxTasks,
            sequelize: this.sequelize,
            table: this.properties.table,
            maxPollCount: this.properties.maxPollCount,
            incrementingColumnName: this.properties.incrementingColumnName,
            timestampColumnName: this.properties.timestampColumnName,
            timestampDelaySeconds: this.properties.timestampDelaySeconds,
            initTimestamp: this.properties.initTimestamp,
            fetchMode: this.properties.fetchMode,
            customQuery: this.properties.customQuery,
            tableSchema: this.tableSchema,
        };

        callback(null, taskConfig);
    }

    stop() {
        this.sequelize.close();
        //sequelizeAuto closes itself after .run() finishes
    }
}

module.exports = SequelizeSourceConnector;