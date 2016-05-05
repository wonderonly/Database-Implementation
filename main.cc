#include "table.h"

#include <sstream>
#include <vector>
#include <functional>
#include <memory>

using namespace std;

shared_ptr<Table> FindTable(string table, vector<shared_ptr<Table>>& tables) {
  shared_ptr<Table> ptr;
  for (auto t : tables) {
    if (t->GetName() == table) {
      ptr = t;
      break;
    }
  }
  return ptr;
}

// main function takes terminal input and parse it
int main(int argc, char *argv[]) {
  string line;
  vector<shared_ptr<Table>> tables;

  cout << "Please load csv files, sample command: "
       << "load a test1.csv" << endl;
  while (getline(cin, line)) {
    istringstream iss(line);
    string operation;
    iss >> operation;

    if (operation == "load") {
      string table, file;
      iss >> table >> file;
      shared_ptr<Table> tablePtr(new Table(file, table));
      tables.push_back(tablePtr);
    } else if (operation == "select") {  // input "select ... from ..."
      string token;
      vector<string> colNames;
      // read in column names
      while (iss >> token) {
        if (token == "from") 
          break;
        colNames.push_back(token);
      }
      if (token != "from") {
        cerr << "Missing 'from' in select clause." << endl;
      }
      // read in table name
      iss >> token;
      shared_ptr<Table> tablePtr = FindTable(token, tables);
      if (!tablePtr) {
        cerr << "Table: " << token << " not found!" << endl;
      } else {
        tablePtr->Projection(colNames);
      }
    } else if (operation == "join") {  // input " ... joinType ... on ..."
      string joinType;
      string table1, table2;
      string att1, att2;
      string on;
      iss >> joinType >> table1 >> att1 >> on >> table2 >> att2;
   
      shared_ptr<Table> tablePtr1 = FindTable(table1, tables);
      shared_ptr<Table> tablePtr2 = FindTable(table2, tables);
      if (!tablePtr1) {
        cerr << "Table: " << table1 << " not found!" << endl;
      } else if (!tablePtr2) {
        cerr << "Table: " << table2 << " not found!" << endl;
      } else {
        if (joinType == "inner") {
          (*tablePtr1).InnerJoin(att1, *tablePtr2, att2);
        } else if (joinType == "outer") {
          (*tablePtr1).OuterJoin(att1, *tablePtr2, att2);
        } else {
          cerr << "Unkown join type!" << endl;
        }
      }      
    } else if (operation == "compute") {  // input arithmetic computation
      string att1, att2;
      string oper, table;
      string from;
      iss >> att1 >> oper >> att2 >> from >> table;
      shared_ptr<Table> tablePtr = FindTable(table, tables);
      if (!tablePtr) {
        cerr << "Table: " << table << " not found!" << endl;
      } else {
        tablePtr->Compute(att1, att2, oper[0]);
      }
    } else if (operation == "aggregation") {
      string agg;
      string att, table;
      string from;
      iss >> agg >> att >> from >> table;
      shared_ptr<Table> tablePtr = FindTable(table, tables);
      if (!tablePtr) {
        cerr << "Table: " << table << " not found!" << endl;
      } else {
        tablePtr->Aggregation(att, agg);
      }
    } else if (operation == "exit") {
      return 0;
    } else {
      cerr << "Unknown operation!" << endl;
      cerr << "Supported operations are: load, select, join, compute, aggregation" 
        << endl;
    }
  }
  return 0;
}
