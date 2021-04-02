# SimpleDB

Course Project of CS392, Database Management System, Spring 2021

## Time Line

### Lab 0

* 2021.03.31	Create repository. Reformat all files.

### Lab 1

* 2021.03.31	Implement class `TupleDesc`.
  * For `numFields()` and `getSize()` (of bytes), I store them as class fields to avoid multiple calculations for multiple queries.
  * I use a mapping from names to their corresponding indices to make `fieldNameToIndex(...)` faster.
  * For `hashCode()`, I first implement `hashCode()` for `TDItem`, which returns the hash code of `fieldType`. Then I convert the array of `TDItem`s to a `List`, and return the hash code of the list.
  * For `toString()`, I use word "NONAME" for those fields whose names are `null`.
* 2021.03.31	Implement class `Tuple`.
  * For `toString()`, I use word "FIELD_NOT_SET" for those fields that have not been set yet.
* 2021.04.02	Implement class `Catalog`.
  * I use the id of a file as the id of its corresponding table.
  * I widely use `HashMap`s to avoid iterating over all the tables when searching.
  * I add a helper function to get a table for `getTupleDesc(...)`, `getDatabaseFile(...)` and `getPrimaryKey(...)`.
* 2021.04.02	Implement class `BufferPool` (the Lab 1 part). (I haven't use any lock so far.)
* 2021.04.02	Implement class `HeapPageId`.

