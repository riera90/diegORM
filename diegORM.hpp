/** LICENCED UNDER THE BSD 3 LICENCE
  *
  * Copyright (c) 2019, Diego Rodríguez Riera All rights reserved.
  *
  * This ORM is intended as a fast way of prototyping, not a final product
  * it is not well made, but for me is a easy way to be able to use a
  * simple ORM at my university projects
  * (witch must be totally coded by me) that's why I made an ORM
  * 
  * if you want to use this ORM, your classes mush implement the ORMInterface interface
  */


#ifndef _DIEGORM_
#define _DIEGORM_

#include <string>
#include <vector>
#include <iostream>
#include <cstring>
#include <sqlite3.h>
#include <string>
#include <tuple>


class SerializableInterface{
public:
    virtual std::string serialize() const = 0;
    virtual bool deserialize(std::string stream) = 0;


    std::vector<std::tuple<std::string, std::string>>
    getTuplesFromStream(const std::string& stream) const
    {
        std::string input = stream;
        if (input[0] != '{') {
            fprintf(stderr, "ERROR, could not deserialize object, expected '{' at the start of stream\n");
            return *(new std::vector<std::tuple<std::string, std::string>>());
        }
        if (input[input.size() - 1] != '}') {
            fprintf(stderr, "ERROR, could not deserialize object, expected '}' at the end of stream\n");
            return *(new std::vector<std::tuple<std::string, std::string>>());
        }

        input = input.substr(1, input.size() - 2);

        int fieldDelimiterIndex;
        int tagDelimiterIndex;

        std::string field;
        std::string tag;
        std::string value;

        auto tuples = new std::vector<std::tuple<std::string, std::string>>();
        while ( !input.empty() ) {
            fieldDelimiterIndex = input.find('|');
            int arrayCompensation = 0;

            if (input.find('[') != std::string::npos && fieldDelimiterIndex > input.find('[')) {
                int oppenedArraysCount = 0;
                bool first = true;
                int lastArraySympolIndex = 0;
                while (oppenedArraysCount != 0 || first) {
                    first = false;
                    int nextArrayOpenIndex = input.find('[', lastArraySympolIndex + 1);
                    int nextArrayCloseIndex = input.find(']', lastArraySympolIndex + 1);
                    if (nextArrayCloseIndex == std::string::npos)
                        nextArrayCloseIndex = 9999999;
                    if (nextArrayOpenIndex == std::string::npos)
                        nextArrayOpenIndex = 9999999;

                    if (nextArrayOpenIndex == nextArrayCloseIndex && nextArrayCloseIndex == 9999999)
                        break;

                    if (nextArrayOpenIndex < nextArrayCloseIndex){
                        lastArraySympolIndex = nextArrayOpenIndex;
                        oppenedArraysCount ++;
                    }else{
                        lastArraySympolIndex = nextArrayCloseIndex;
                        oppenedArraysCount --;
                    }
                    arrayCompensation = 1;
                }
                fieldDelimiterIndex = lastArraySympolIndex;
            }
            // FIXME: clean this pasta party
            if (fieldDelimiterIndex != std::string::npos) {
                field = input.substr(0, fieldDelimiterIndex + arrayCompensation);
                if (fieldDelimiterIndex + 1 >= input.size() )
                    arrayCompensation = 0;
                input = input.substr(fieldDelimiterIndex + 1 + arrayCompensation, input.size());
            } else {
                field = input;
                input.clear();
            }

            tagDelimiterIndex = field.find(':');
            if (tagDelimiterIndex == std::string::npos) {
                fprintf(stderr, "ERROR, could not deserialize object, expected ':' as tag:value separator\n");
                return *(new std::vector<std::tuple<std::string, std::string>>());
            }

            tag = field.substr(0, tagDelimiterIndex);
            value = field.substr(tagDelimiterIndex + 1, field.size());
            /*std::cout<<"field<"<<field<<">\n";
            std::cout<<"input<"<<input<<">\n";
            std::cout<<"tag<"<<tag<<">\n";
            std::cout<<"value<"<<value<<">\n\n";*/
            tuples->emplace_back(tag, value);
        }

        return *tuples;
    }



    std::vector<std::string>
    getStreamsFromSerializedInput(const std::string& serializedInput) const
    {
        std::vector<std::string> streams;
        std::string stream;
        std::string input = serializedInput;
        int streamDelimiterIndex;
        while ( !input.empty() ) {
            int oppenedArraysCount = 0;
            bool first = true;
            int lastArraySympolIndex = 0;
            streamDelimiterIndex = input.find(',');
            int arrayCompensation = 0;

            if (input.find('[') != std::string::npos && streamDelimiterIndex > input.find('[')) {
                while (oppenedArraysCount != 0 || first) {
                    first = false;
                    int nextArrayOpenIndex = input.find('[', lastArraySympolIndex + 1);
                    int nextArrayCloseIndex = input.find(']', lastArraySympolIndex + 1);
                    if (nextArrayCloseIndex == std::string::npos)
                        nextArrayCloseIndex = 9999999;
                    if (nextArrayOpenIndex == std::string::npos)
                        nextArrayOpenIndex = 9999999;

                    if (nextArrayOpenIndex == nextArrayCloseIndex && nextArrayCloseIndex == 9999999)
                        break;

                    if (nextArrayOpenIndex < nextArrayCloseIndex){
                        lastArraySympolIndex = nextArrayOpenIndex;
                        oppenedArraysCount ++;
                    }else{
                        lastArraySympolIndex = nextArrayCloseIndex;
                        oppenedArraysCount --;
                    }
                    arrayCompensation = 1;

                }
                streamDelimiterIndex = input.find(',', lastArraySympolIndex + 1);
            }



            if (streamDelimiterIndex != std::string::npos) {
                stream = input.substr(0, streamDelimiterIndex);
                input = input.substr(streamDelimiterIndex + 1, input.size());
            } else {
                stream = input;
                input.clear();
            }
            /*std::cout<<"stream<"<<stream<<">\n";
            std::cout<<"input<"<<input<<">\n";*/
            streams.emplace_back(stream);

        }

        return streams;
    }


    std::vector<std::vector<std::tuple<std::string, std::string>>>
    getTuplesCollectionFromStream(const std::string &stream) const
    {
        std::vector<std::vector<std::tuple<std::string, std::string>>> tuplesCollection;

        for (auto& splitedStream : getStreamsFromSerializedInput(stream)) {
            tuplesCollection.emplace_back(getTuplesFromStream(splitedStream));
        }

        return tuplesCollection;
    }
};








class ORMInterface: public SerializableInterface{
protected:
    int id_;
    std::string tableName_;
public:
    ORMInterface() : id_(-1), tableName_(""), SerializableInterface() {};
    const std::string& getTableName() const {return this->tableName_;};
    int getId() const {return this->id_;};
};


class ORM{
private:
    std::string stream_;
    sqlite3* db_;

private:
    /*static int genericCallback(void *NotUsed, int argc, char **argv, char **azColName) {
        for(int i = 0; i<argc; i++)
            printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "nullptr");
        printf("\n"); return 0;
    }*/

    static int selectCallback(void *stream, int argc, char **argv, char **azColName) {
        auto buffer = (std::string*) stream;
        if (!buffer->empty())
            (*buffer) += ",";
        (*buffer) += "{";
        for(int i = 0; i<argc; i++) {
            if (i != 0)
                (*buffer) += "|";
            (*buffer) += std::string(azColName[i]) + ":" + std::string(argv[i]);
        }
        (*buffer) += "}";
        return 0;
    }


public:
    explicit ORM(const std::string& dbName) {
        int rc = sqlite3_open(dbName.c_str(), &this->db_);

        if( rc ) {
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db_));
            exit(0);
        }
    };

    ~ORM() {
        sqlite3_close(db_);
    };

    template <class ObjectType>
    bool createTable() {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        ObjectType object;
        std::string querry = "CREATE TABLE "+ object.getTableName() +"(id INT PRIMARY KEY ";
        auto tuples = object.getTuplesFromStream(object.serialize());
        for (int i = 1; i < tuples.size(); ++i) {
            querry += ", " + std::get<0>(tuples[i]) + " CHAR(255)";
        }
        querry += ");";

        char *zErrMsg = nullptr;
        int rc;
        char *sql;

        rc = sqlite3_exec(db_, querry.c_str(), nullptr, nullptr, &zErrMsg);

        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            return false;
        }
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
        return true;
    };



    template <class ObjectType>
    int save(ObjectType object) {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        std::string querry = "INSERT INTO "+ object.getTableName() + " VALUES (";
        int id = object.getId();
        if ( id == -1 ) {
            auto allObjects = this->all<ObjectType>();
            for (auto objectAux : allObjects) {
                if (objectAux.getId() > id)
                    id = objectAux.getId();
            }
            id++;
        }
        querry += std::to_string(id);
        auto tuples = object.getTuplesFromStream(object.serialize());
        for (int i = 1; i < tuples.size(); ++i) {
            querry += ", '" + std::get<1>(tuples[i]) + "'";
        }
        querry += ");";
        char *zErrMsg = nullptr;
        int rc;
        char *sql;
        rc = sqlite3_exec(db_, querry.c_str(), selectCallback, &this->stream_, &zErrMsg);
        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            return -1;
        }
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);

        return id;
    };



    template <class ObjectType>
    std::vector<ObjectType> all() {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        this->stream_.clear();
        ObjectType object;
        std::vector<ObjectType> objects;
        std::string querry = "select * from "+ object.getTableName() + ";";
        char *zErrMsg = nullptr;
        int rc;
        char *sql;
        rc = sqlite3_exec(db_, querry.c_str(), selectCallback, &this->stream_, &zErrMsg);
        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            return objects;
        }
        auto streams = object.getStreamsFromSerializedInput(this->stream_);
        for (auto& auxStream : streams) {
            object.deserialize(auxStream);
            objects.emplace_back(object);
        }
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
        return objects;
    };

    template <class ObjectType>
    ObjectType getById(int id) {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        ObjectType object;
        this->stream_.clear();
        std::string querry = "select * from "+ object.getTableName();
        querry += " where id is "+ std::to_string(id) +";";
        char *zErrMsg = nullptr;
        int rc;
        char *sql;
        rc = sqlite3_exec(db_, querry.c_str(), selectCallback, &this->stream_, &zErrMsg);
        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            return object;
        }
        object.deserialize(this->stream_);
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
        return object;
    };


    /**
     * this function can behave unpredictably, but the object id is always correct, check the veracity of the data
     * with the getById funciton
     * @tparam ObjectType
     * @param field
     * @param value
     * @return vector of filtered objects
     */
    template <class ObjectType>
    std::vector<ObjectType> getByField(const std::string& field, const std::string& value) {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        this->stream_.clear();
        ObjectType object;
        std::vector<ObjectType> objects;
        std::string querry = "select * from "+ object.getTableName();
        querry += " where " + field + " is '"+ value +"';";

        char *zErrMsg = nullptr;
        int rc;
        char *sql;
        rc = sqlite3_exec(db_, querry.c_str(), selectCallback, &this->stream_, &zErrMsg);
        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            return objects;
        }
        auto streams = object.getStreamsFromSerializedInput(this->stream_);
        for (auto& auxStream : streams) {
            object.deserialize(auxStream);
            objects.emplace_back(object);
        }
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
        return objects;
    };

    template <class ObjectType>
    bool update(ObjectType object) {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        this->stream_.clear();
        std::string querry = "update "+ object.getTableName() + " set ";
        bool firstField = true;
        auto tuples = object.getTuplesFromStream(object.serialize());
        for (int i = 1; i < tuples.size(); ++i) {
            if ( i != 1 )
                querry += ", ";
            querry += std::get<0>(tuples[i]) + " = '" + std::get<1>(tuples[i]) + "'";
        }
        querry += " where id is " + std::to_string(object.getId()) + ";";
        char *zErrMsg = nullptr;
        int rc;
        char *sql;
        rc = sqlite3_exec(db_, querry.c_str(), nullptr, &this->stream_, &zErrMsg);
        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            return false;
        }
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
        return true;
    };


    template <class ObjectType>
    bool remove(ObjectType object) {
        sqlite3_exec(this->db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        this->stream_.clear();
        std::string querry = "delete from "+ object.getTableName() + " where id is " + std::to_string(object.getId()) + ";";
        bool firstField = true;
        auto tuples = object.getTuplesFromStream(object.serialize());

        char *zErrMsg = nullptr;
        int rc;
        char *sql;
        rc = sqlite3_exec(db_, querry.c_str(), selectCallback, &this->stream_, &zErrMsg);
        if( rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
            sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
            return false;
        }
        sqlite3_exec(this->db_, "END TRANSACTION;", nullptr, nullptr, nullptr);
        sqlite3_exec(this->db_, "COMMIT;", nullptr, nullptr, nullptr);
        return true;
    };

};

#endif
