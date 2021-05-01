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
  * You can directly see how long I spent by looking at this timeline.

### Lab 2

* 2021.04.07	Initialize Lab 2.
* 2021.04.07	Add `Pair` utility.
* 2021.04.07	Implement page eviction in class `BufferPool`.
  * For eviction policy, I use LRU. I add a timer of type `long`, which represents the number of reads. Each time if eviction is needed, I select a page with minimum accessed time and evict the page.
* 2021.04.09	Implement class `Predicate` and `IndexPredicate`.
  * So far I don't know what `IndexPredicate` is used for, and I cannot understand the document of `IndexPredicate#equal(...)`.
* 2021.04.09	Implement method `findLeafPage(...)` in class `BTreeFile`.
  * To meet the requirement that "when a key exists in multiple leaf pages, `findLeafPage(...)` should return the left-most leaf page", in my B+ tree a key of a `BTreeEntry` is the maximum key in its left child. I cannot think of other way.
* 2021.04.10	Implement B+ tree insertion. The insertion mainly involves `splitLeafPage(...)` and `splitInternalPage(...)`. I've written my steps in class `BTreeFile`.
  * For `splitLeafPage(...)`, the steps are:
    0. Mark the page as dirty.
    1. Fetch the tuples to be moved.
    2. Get the middle entry key which would be copied up to the parent.
    3. Create the new page.
    4. Move the tuples.
    5. Fetch the parent page by calling `getParentWithEmptySlots()`. Note that the recursive split happens in this method.
    6. Update the parent pointers.
    7. Insert the new entry to the parent.
    8. Update the sibling pointers.
    9. Return the desired page.
  * For `splitInternalPage(...)`, the steps are similar. The differences are that we need to set the new children of `midEntry` and we don't need to update any sibling pointers.
* 2021.04.10	Implement B+ tree deletion. The deletion involves three "steal"s and two "merge"s. I've also written my steps in class `BTreeFile`.
  * For `stealFromLeafPage(...)`, the steps are:
    1. Fetch the tuples to be moved.
    2. Mote the tuples.
    3. Update the parent entry.
  * For `stealFromLeftInternalPage(...)` and `stealFromRightInternalPage(...)`, the steps are:
    1. Fetch the entries to be moved.
    2. Mark three pages as dirty.
    3. Move the entries.
       * Get the parent key and the key of the entry to be moved.
       * Remove the entry from the left/right sibling page.
       * Update the entry and insert it to the right page.
       * Update the parent pointer of the moved page.
       * Update the key of the parent entry.
  * For `mergeLeafPages(...)`, the steps are:
    1. Fetch the tuples to be moved.
    2. Mark three pages as dirty.
    3. Remove the parent entry. Note that the recursive stealing or merging might happen in this method.
    4. Move the tuples.
    5. Update the sibling pointers.
    6. Set the right page as empty.
  * For `mergeInternalPages(...)`, the steps are almost the same. The differences are that we need to update the parent pointers of the moved pages after merging, and we don't need to update the sibling pointers.
* 2020.04.10	Finish Lab 2.
  * All the decisions that worth telling are described above. Seems that there isn't any bonus exercise.
  * I didn't change any API.
  * I implemented all the code blank needed for Lab 1, and it can pass all the basic tests and system tests of Lab 1 and Lab 2.
  * You can directly see how long I spent by looking at this timeline.

### Lab 3

* 2021.04.19	Initialize Lab 3.
* 2021.04.19	Implement class `JoinPredicate`.
* 2021.04.19	Implement operator Filter.
* 2021.04.27	Fix a bug in `SeqScan`. This bug will be triggered when closing an already closed `SeqScan` iterator.
* 2021.04.27	Implement operator Join. To avoid duplicate calculation of the `TupleDesc` of the joined table, I calculate the `TupleDesc` immediately after setting new `child` or `child2`. I adopt the simplest join strategy. The details are as followed:
  1. I use a field `curChild1` to record the current tuple of `child1`. Initially it is `null`.
  2. When `fetchNext()` is invoked, we initialize `curChild1` if it is `null`: if `child1` has a next tuple, let `curChild1` be the next tuple. Otherwise we return `null`. 
  3. Each time when `fetchNext()` is invoked, we iterate over `child2`, trying to find a pair of expected tuple. If found, we return the tuple joined by the pair. Otherwise we check whether `child1` has a next tuple. If so, we set `curChild1` to be the next tuple of `child1` and rewind `child2`. If not, we return `null`.
* 2021.04.28	Fix a bug in method `toString()` of class `Tuple`, which used byte size as the number of fields.
* 2021.04.28	Implement `IntegerAggregator`. I leverage the feature that `null` can be a key of a `Map`. So I use a map to recording the aggregate value for each group value. When `gbfield` is `NO_GROUPING`, I use `null` as the group value for all tuples.
* 2021.04.28	Implement `StringAggregator`. The design is almost the same as `IntegerAggregator`.
* 2021.05.01	Implement operator Aggregate. I do the aggregation when this operator gets opened, and creates an iterator of the aggregator after the aggregation. When invoking `fetchNext()`, I just get the next tuple of the iterator.
* 2021.05.01	Implement tuple insertion and deletion in class `HeapPage` and `HeapFile`.