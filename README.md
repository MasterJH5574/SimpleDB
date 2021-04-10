# SimpleDB

Course Project of CS392, Database Management System, Spring 2021

## Timeline

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
* 2021.04.02	Implement class `RecordId`.
* 2021.04.03	Implement class `HeapPage` (the Lab 1 part).
* 2021.04.03	Implement class `HeapFile` (the Lab 1 part).
  * For `iterator(...)`, I record a current page number of the iterator and the `Iterator<Tuple>` of the current page. I also write a `fetchPage` method which takes a page number as argument, reads the page into buffer pool, and finally returns the iterator of the tuples in that page. In this way I can utilize the iterator implemented in `HeapPage`.
* 2021.04.03	Implement class `SeqScan`. We can find the file of the table by looking up the catalog. After finding the file, we acquire its `DbFileIterator`. Then almost all the code of `DbIterator` uses the `DbFileIterator` of the underlying file.
* 2021.04.03	Finish Lab 1.
  * All the decisions that worth telling are described above. Other decisions are really trivial, so I don't want to talk about them.
  * I didn't change any API.
  * I implemented all the code blank for Lab 1.
  * You can directly see how long I spent by looking this timeline.

### Lab 2

* 2021.04.07	Initialize Lab 2.
* 2021.04.07	Add `Pair` utility.
* 2021.04.07	Implement page eviction in class `BufferPool`.
  * For eviction policy, I use LRU. I add a timer of type `long`, which represents the number of reads. Each time if eviction is needed, I select a page with minimum accessed time and evict the page.
* 2021.04.09	Implement class `Predicate` and `IndexPredicate`.
  * So far I don't know what `IndexPredicate` is used for, and I cannot understand the document of `IndexPredicate#equal(...)`.
* 2021.04.09	Implement method `findLeafPage(...)` in class `BTreeFile`.
  * To meet the requirement that "when a key exists in multiple leaf pages, `findLeafPage(...)` should return the left-most leaf page", in my B+ tree a key of a `BTreeEntry` is the maximum key in its left child. I cannot think of other way.
* 2021.04.10	Implement B+ tree insertion.
* 2021.04.10	Implement B+ tree deletion.

