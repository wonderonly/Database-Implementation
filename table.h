#ifndef TABLE_H
#define TABLE_H
#include <iostream>
#include <vector>
#include <string>
#include <fstream>

using namespace std;

class Table {

public:
  // define constructor
  Table (string name, string tableName);

  string GetName() {return _tableName;}

  void Projection(const vector<string>& colNames);
  // compute +, -, *, / between two columns
  void Compute(string col1, string col2, char op);
  // calculate aggregations: min, max, average, median
  void Aggregation(string colName, string agg);
  // join on attributes myColName and otherColName
  void InnerJoin(string myColName, const Table& other, string otherColName);
  void OuterJoin(string myColName, const Table& other, string otherColName);

private:
  // disable default constructor
  Table();

  // table name
  string _tableName;
  // csv file name
  string _fileName;
  fstream _file;
  // column names
  vector<string> _headers;
  // actual data set
  vector< vector<double>> _table;

  /* extract a column out of a table together with the tuple_id/tuple_postions
  *  for joins.*/
  vector<pair<double, int>> ExtractColumn(int colId, const Table& table);
  // given a column name, return its column id
  int FindColumn(string colName) const;
  // parse csv file
  void Parse();
  // print the column names
  void PrintHeaders(const vector<string>& headers);
};

#endif
