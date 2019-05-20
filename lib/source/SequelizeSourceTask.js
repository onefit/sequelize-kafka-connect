"use strict";

const { SourceTask, SourceRecord } = require("kafka-connect");

const MODE_INCREMENT = 'increment';
const MODE_TIMESTAMP = 'timestamp';
const DEFAULT_ID_COLUMN = 'id';
const DEFAULT_UPDATED_AT_COLUMN = 'updated_at';

class SequelizeSourceTask extends SourceTask {

    _getIncrementCondition() {
        return [
            "SELECT",
            "*",
            "FROM",
            this.quoteFunction(this.table),
            "ORDER BY",
            this.quoteFunction(`${this.table}.${this.incrementingColumn}`),
            "LIMIT",
            ":limit",
            "OFFSET",
            ":offset"
        ];
    }

    _getTimeStampCondition() {
        return [
            `((${this.quoteFunction(`${this.table}.${this.incrementingColumn}`)} = :timestamp`,
            "AND",
            `${this.quoteFunction(`${this.table}.${this.incrementingColumn}`)} > :lastid ) OR ${this.quoteFunction(`${this.table}.${this.timestampColumn}`)} > :timestamp)`,
            "ORDER BY",
            `${this.quoteFunction(`${this.table}.${this.timestampColumn}`)}, ${this.quoteFunction(`${this.table}.${this.incrementingColumn}`)}`,
            "LIMIT",
            ":limit",
            "OFFSET",
            ":offset"
        ];
    }

    _getQuery() {
        let query = [];
        if (this.customQuery) {
            const customQuery = this.customQuery.replace(/(ORDER|GROUP BY|LIMIT|OFFSET)(.+)$/mg, '');
            query =[ customQuery, (customQuery.toLowerCase().indexOf(' where ') === -1 ? 'WHERE' : 'AND') ];
        } else {
            query = [ "SELECT", "*", "FROM", this.quoteFunction(this.table, true), "WHERE" ];
        }

        if (this.fetchMode === MODE_INCREMENT) {
            return [...query, ...this._getIncrementCondition()];
        } else if (this.fetchMode === MODE_TIMESTAMP) {
            return [...query, ...this._getTimeStampCondition()];
        } else {
            throw Error(`Incorrect fetch mode - ${this.fetchMode}, available: [${MODE_INCREMENT}, ${MODE_TIMESTAMP}]`);
        }
    }

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
            customQuery,
            fetchMode,
        } = this.properties;

        this.sequelize = sequelize;
        this.maxTasks = maxTasks;
        this.table = table;
        this.maxPollCount = maxPollCount;
        this.incrementingColumn = incrementingColumnName || DEFAULT_ID_COLUMN;
        this.timestampColumn = timestampColumnName || DEFAULT_UPDATED_AT_COLUMN;
        this.tableSchema = tableSchema;
        this.customQuery = customQuery;
        this.quoteFunction = function (op) {
            const func = this.sequelize.dialect.QueryGenerator.quoteIdentifier;
            if (op.indexOf('.') === -1) {
                return func(op, true);
            }
            return func(op.split('.')[0], true) + '.' + func(op.split('.')[1], true);
        };

        this.fetchMode = fetchMode || MODE_INCREMENT;

        this.currentOffset = 0;
        this.lastId = 0;
        this.lastTimestamp = 0;

        callback(null);
    }

    poll(callback) {
        const query = this._getQuery();

        this.sequelize.query(
            query.join(" "), {
                type: this.sequelize.QueryTypes.SELECT,
                replacements: {
                    lastid: this.lastId,
                    timestamp: this.lastTimestamp,
                    limit: this.maxPollCount,
                    offset: this.fetchMode === MODE_INCREMENT ? this.currentOffset : 0,
                }
            }
        ).then(results => {

            this.currentOffset += results.length;
            console.log(`${results.length} published`);

            const records = results.map(result => {

                const record = new SourceRecord();

                record.key = result[this.incrementingColumn];
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
                this.lastTimestamp = result[this.timestampColumn];
                this.lastId = result[this.incrementingColumn];
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