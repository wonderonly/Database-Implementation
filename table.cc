#include "table.h"

#include <sstream>
#include <functional>
#include <algorithm>

Table::Table(string fileName, string tableName) : _fileName(fileName), 
  _tableName(tableName) {
  _file.open(_fileName.c_str(), fstream::in);
  if (!_file.good()) {
    cerr << "Table reading error for file : " << _fileName << endl; 
  }
  Parse();
}

// implement projection
void Table::Projection(const vector<string>& colNames) {
  vector<int> colIds;
  for (auto colName : colNames) {
    colIds.push_back(FindColumn(colName));
    // if column does not exit, return
    if (colIds.back() == -1)
      return;
  }

  // print headers
  string sheaders (""), dashes;
  for (auto colName : colNames) {
    dashes += "--------";
    sheaders += _tableName + "." + colName + "\t";
  }
  cout << dashes << endl;
  cout << sheaders << endl;
  cout << dashes << endl;
  // print data
  for (auto line : _table) {
    for (auto colId : colIds) {
      cout << line[colId] << "\t";
    }
    cout << endl;
  }
}

// implement arithmetic computatons on two columns
void Table::Compute(string col1, string col2, char oper) {
  int colId1, colId2;
  colId1 = FindColumn(col1);
  colId2 = FindColumn(col2);

  if (colId1 != -1 && colId2 != -1) {
    std::function<double(double, double)> fn;
    switch (oper) {
      case '+': fn = [](double x, double y) {return x + y;}; break; 
      case '-': fn = [](double x, double y) {return x - y;}; break;
      case '*': fn = [](double x, double y) {return x * y;}; break;
      case '/': fn = [](double x, double y) {return x / y;}; break;
    }
    for (auto values : _table) {
      cout << fn(values[colId1], values[colId2]) << endl;
    }
  }
}

// implement aggregations: min, max, average, and median
void Table::Aggregation(string colName, string agg) {
  int colId = FindColumn(colName);

  if (colId != -1) {
    double ans;
    if (agg == "min") {
      ans = _table[0][colId];
      for (auto values : _table) {
        ans = min(ans, values[colId]);
      }
    } else if (agg == "max") {
      ans = _table[0][colId];
      for (auto values : _table) {
        ans = max(ans, values[colId]);
      } 
    } else if (agg == "average") {
      ans = _table[0][colId];
      for (auto values : _table) {
        ans += values[colId];
      } 
      ans /= _table.size(); 
    } else if (agg == "median") {
      vector<double> temp;
      for (auto values : _table) {
        temp.push_back(values[colId]);
      } 
      sort(temp.begin(), temp.end());
      ans = temp.size() % 2 == 0 ?
        (temp[temp.size() / 2] + temp[temp.size() / 2 - 1]) / 2 :
        temp[temp.size() / 2];
    }
    cout << agg << ": " << ans << endl;
  }
}

// implement inner join
void Table::InnerJoin(string myColName, const Table& other, string otherColName) {
  int myColId = FindColumn(myColName);
  int otherColId = other.FindColumn(otherColName);
  if (myColId == -1 || otherColId == -1) {
    return;
  }
 
  // produce combined headers
  vector<string> headers(_headers);
  for (auto& h : headers) {
    h = _tableName + "." + h;
  }
  for (auto& h : other._headers) {
    headers.push_back(other._tableName + "." + h);
  }

  /* do the join */
  // step 1, get columns from tables
  vector<pair<double, int>> myCol = ExtractColumn(myColId, *this);
  vector<pair<double, int>> otherCol = ExtractColumn(otherColId, other);

  // step 2, sort-merge join is applied here
  sort(myCol.begin(), myCol.end(),
    [](pair<double, int> lhs, pair<double, int> rhs) {
      return lhs.first < rhs.first;
    });

  sort(otherCol.begin(), otherCol.end(),
    [](pair<double, int> lhs, pair<double, int> rhs) {
      return lhs.first < rhs.first;
    });
 
  // step 3, store tuple id pairs between two tables
  vector<pair<int, int>> result;
  /* [left, right) denotes index range in other table that mathes with current
   * value in my table
   */
  int left, right;
  left = right = 0;
  for (auto p : myCol) {
    while (right < otherCol.size() && otherCol[right].first <= p.first) {
      right++;
    }
    while (left < right && otherCol[left].first < p.first) {
      left++;
    }
    for (int i = left; i < right; i++) {
      result.push_back(pair<int,int>(p.second, otherCol[i].second));
    }
  }
  // step 4, print headers
  PrintHeaders(headers);
  // print the joint result
  for (auto p : result) {
    for (int i = 0; i < _table[p.first].size(); i++) {
      cout << _table[p.first][i] << "\t";
    }
    for (int i = 0; i < _table[p.second].size(); i++) {
      cout << _table[p.second][i] << "\t";
    }
    cout << endl;
  }
  if (result.empty()) {
    cout << "no match found!" << endl;
  }
}

// implement outer join
void Table::OuterJoin(string myColName, const Table& other, string otherColName) {
  int myColId = FindColumn(myColName);
  int otherColId = other.FindColumn(otherColName);
  if (myColId == -1 || otherColId == -1) {
    return;
  }
  
  // produce combined headers
  vector<string> headers(_headers);
  for (auto& h : headers) {
    h = _tableName + "." + h;
  }
  for (auto& h : other._headers) {
    headers.push_back(other._tableName + "." + h);
  }

  /* do the join */
  // step 1, get columns from two tables
  vector<pair<double, int>> myCol = ExtractColumn(myColId, *this);
  vector<pair<double, int>> otherCol = ExtractColumn(otherColId, other);

  // step 2, sort-merge join is applied here
  sort(myCol.begin(), myCol.end(),
    [](pair<double, int> lhs, pair<double, int> rhs) {
      return lhs.first < rhs.first;
    });

  sort(otherCol.begin(), otherCol.end(),
    [](pair<double, int> lhs, pair<double, int> rhs) {
      return lhs.first < rhs.first;
    });
 
  // step 3, store tuple id pairs between two tables
  vector<pair<int, int>> result;
  /* [left, right) denotes index range in other table that mathes with current
   * value in my table
   */
  int left, right;
  left = right = 0;
  for (auto p : myCol) {
    while (right < otherCol.size() && otherCol[right].first <= p.first) {
      if (otherCol[right].first < p.first) {
        // number in other table does not find match in my table
        result.push_back(pair<int, int>(-1, otherCol[right].second));
      }
      right++;
    }
    while (left < right && otherCol[left].first < p.first) {
      left++;
    }
    
    // numbers in my table does not find any match in other table 
    if (left == right) {
      result.push_back(pair<int, int>(p.second, -1));
    } else {
      // found matches
      for (int i = left; i < right; i++) {
        result.push_back(pair<int,int>(p.second, otherCol[i].second));
      }
    }
  }
  // numbers in other table does not find any match in my table
  while (right < otherCol.size()) {
    result.push_back(pair<int, int>(-1, otherCol[right].second));
    right++;
  }

  // step 4, print headers
  PrintHeaders(headers);
  // print the joint result
  for (auto p : result) {
    if (p.first != -1) {
      for (int i = 0; i < _table[p.first].size(); i++) {
        cout << _table[p.first][i] << "\t";
      }
    } else {
      for (int i = 0; i < _headers.size(); i++) {
        cout << "\t";
      }
    }
    if (p.second != -1) {
      for (int i = 0; i < other._table[p.second].size(); i++) {
        cout << other._table[p.second][i] << "\t";
      }
    } else {
      for (int i = 0; i < other._headers.size(); i++) {
        cout << "\t";
      }
    }
    cout << endl;
  }
}

vector<pair<double,int>> Table::ExtractColumn(int colId, const Table& table) {
  vector<pair<double,int>> column;
  for (int i = 0; i < table._table.size(); i++) {
    column.push_back (pair<double,int>(table._table[i][colId], i));
  }
  return column;
}

int Table::FindColumn(string colName) const {
  int columnId = -1;
  for (int i = 0; i < _headers.size(); i++) {
    if(_headers[i] == colName) {
      columnId = i;
    }
  }
  if (columnId == -1) {
    cerr << "Column " << colName << " not found." << endl;
  }
  return columnId;
}

void Table::Parse() {
  string line;
  string temp;

  // get headers
  getline(_file, line);
  istringstream iss(line);
  while (getline(iss, temp, ',')) {
    _headers.push_back (temp);
  }
  
  // get values
  while (getline(_file, line)) {
    istringstream iss(line);
    vector<double> values;
    while (getline(iss, temp, ',')) {
      values.push_back (atof(temp.c_str()));
    }
    _table.push_back (values);
  }
}

void Table::PrintHeaders(const vector<string>& headers) {
  string sheaders (""), dashes;
  for (auto colName : headers) {
    dashes += "--------";
    sheaders += colName + "\t";
  }
  cout << dashes << endl;
  cout << sheaders << endl;
  cout << dashes << endl;
}
