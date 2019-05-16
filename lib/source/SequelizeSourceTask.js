"use strict";

const { SourceTask, SourceRecord } = require("kafka-connect");

const MODE_INCREMENT = 'increment';
const MODE_TIMESTAMP = 'timestamp';
const DEFAULT_ID_COLUMN = 'id';
const DEFAULT_UPDATED_AT_COLUMN = 'updated_at';

class SequelizeSourceTask extends SourceTask {

    start(properties, callback, parentConfig) {

        this.parentConfig = parentConfig;

        this.properties = properties;
        const {
            sequelize,
            maxTasks,
            table,
            maxPollCount,
            incrementingColumnName,
            timestampColumnName,
            tableSchema,
            fetchMode,
        } = this.properties;

        this.sequelize = sequelize;
        this.maxTasks = maxTasks;
        this.table = table;
        this.maxPollCount = maxPollCount;
        this.incrementingColumnName = incrementingColumnName;
        this.timestampColumnName = timestampColumnName;
        this.tableSchema = tableSchema;

        this.fetchMode = fetchMode || MODE_INCREMENT;

        this.currentOffset = 0;
        this.lastId = 0;
        this.lastTimestamp = 0;

        callback(null);
    }

    poll(callback) {

        const idColumn = this.incrementingColumnName || DEFAULT_ID_COLUMN;
        const timestampColumn = this.timestampColumnName || DEFAULT_UPDATED_AT_COLUMN;
        const quoteFunction = this.sequelize.dialect.QueryGenerator.quoteIdentifier;
        let query;

        if (this.fetchMode === MODE_INCREMENT) {
            query = [
                "SELECT",
                "*",
                "FROM",
                quoteFunction(this.table, true),
                "ORDER BY",
                quoteFunction(idColumn, true),
                "LIMIT",
                ":limit",
                "OFFSET",
                ":offset"
            ];
        } else if (this.fetchMode === MODE_TIMESTAMP) {
            query = [
                "SELECT",
                "*",
                "FROM",
                quoteFunction(this.table, true),
                "WHERE",
                `((${quoteFunction(timestampColumn, true)} = :timestamp`,
                "AND",
                `${quoteFunction(idColumn, true)} > :lastid ) OR ${quoteFunction(timestampColumn, true)} > :timestamp)`,
                "ORDER BY",
                `${quoteFunction(timestampColumn, true)}, ${quoteFunction(idColumn, true)}`,
                "LIMIT",
                ":limit",
                "OFFSET",
                ":offset"
            ];
        } else {
            throw Error(`Incorrect fetch mode - ${this.fetchMode}, available: [${MODE_INCREMENT}, ${MODE_TIMESTAMP}]`);
        }

        this.sequelize.query(
            query.join(" "), {
                type: this.sequelize.QueryTypes.SELECT,
                replacements: {
                    lastid: this.lastId,
                    timestamp: this.lastTimestamp,
                    limit: this.maxPollCount,
                    offset: this.fetchMode === MODE_INCREMENT ?  this.currentOffset : 0,
                }
            }
        ).then(results => {

            this.currentOffset += results.length;
            console.log(`${results.length} published`);

            const records = results.map(result => {

                const record = new SourceRecord();

                record.key = result[idColumn];
                record.keySchema = null;

                if (!record.key) {
                    throw new Error("db results are missing incrementing column name or default 'id' field.");
                }

                record.value = result;
                record.valueSchema = this.tableSchema;

                record.timestamp = new Date().toISOString();
                record.partition = -1;
                record.topic = this.table;

                this.parentConfig.emit("record-read", record.key.toString());
                this.lastTimestamp = result[timestampColumn];
                this.lastId = result[idColumn];
                return record;
            });

            callback(null, records);
        }).catch(error => {
            callback(error);
        });
    }

    stop() {
        //empty (con is closed by connector)
    }
}

module.exports = SequelizeSourceTask;