#include <iostream>

#define INFO "[[INFO]] "
#define ERR  "[[ERROR]] "
#define WARN "[[WARN]] "
#define DEBUG "[[DEBUG]] "
#define PrintFnInfo printf(INFO "Currently Testing: %s\n", __PRETTY_FUNCTION__)

template <typename T> 
void test() {}

void testCreationOfTupleMVTO() {
	printf(INFO "Testing Creation Of Tuple MVTO\n");
	Relation<MVTO> Relation;
	using TupleCC = Tuple<MVTO>;

	Transaction txn1;
	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(t1);
	printf(INFO "Successfully inserted tuple at position %llu\n", pos);
}

void testCreateReadUpdateTupleMVTO() {
	Relation<MVTO> Relation;
	using TupleCC = Tuple<MVTO>;

	Transaction txn1;
	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(t1);
	printf(INFO "Inserted tuple at position %llu\n", pos);

	// transaction 2 tries to update it
	Transaction txn2;
	TupleCC& tup1 = Relation[pos]; // Get the original tuple
	assert(tup1.cc.IsValid(txn2) && "This is a valid update");
	tup1.cc.Read(txn2);
	printf(INFO "Read tuple at position %llu\n", pos);

	TupleCC tup2 = TupleCC(txn2, tup1); // Create new Tuple
	tup2.b = 3.0f; // Changes
	printf(INFO "Updated tuple at position %llu\n", pos);

	bool success = tup1.cc.TryLock(txn2); // Lock the tuple for no more changes
	assert(success && "This lock should succeed"); 
	printf(INFO "Locked tuple for updating %llu\n", pos);

	uint64 newPos = Relation.Insert(tup2); // Insert
	tup1.cc.Retire(txn2); // Retire the tuple
	tup1.cc.Unlock();
	printf(INFO "Unlocked and retired tuple at position %llu\n", pos);
	printf(INFO "Successfully inserted new tuple at position %llu\n", newPos);
}

void testCreateReadUpdateTupleByMultipleTransactionMVTO() {
	Relation<MVTO> Relation;
	using TupleCC = Tuple<MVTO>;

	Transaction Txn3, Txn4, Txn5;
	bool success;

	TupleCC _tup3 = TupleCC(Txn3, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(_tup3);

	// Only latest reader could update it
	TupleCC& tup3 = Relation[pos];
	tup3.cc.Read(Txn4);
	printf(INFO "Transaction 4 read tuple at position %llu\n", pos);
	tup3.cc.Read(Txn5);
	printf(INFO "Transaction 5 read tuple at position %llu\n", pos);

	success = tup3.cc.TryLock(Txn4);
	assert(!success && "This lock should not succeed"); 
	printf(INFO "Transaction 4 fails to lock tuple for update as it is read by transaction 5\n");

	success = tup3.cc.TryLock(Txn5);
	assert(success && "This lock should succeed"); 
	printf(INFO "Transaction 5 locks tuple for update\n");

	TupleCC tup4 = TupleCC(Txn4, tup3);
	// --------------------
	tup4.b = 3; // Update
	// --------------------

	uint64 newPos = Relation.Insert(tup4);
	printf(INFO "New tuple is inserted at %llu\n", newPos);

	success = tup3.cc.Retire(Txn5);
	assert(success && "This retire should succeed");
	printf(INFO "Transaction 5 retires tuple at position %llu\n", pos);
}

template <>
void test<MVTO>() {
	PrintFnInfo;
	using TestFns = void (*)();
	TestFns tests[] = {
		testCreationOfTupleMVTO,
		testCreateReadUpdateTupleMVTO,
		testCreateReadUpdateTupleByMultipleTransactionMVTO,
	};

	for (auto test : tests) {
		test();
	}
}

void testCreationOfTupleMV2PL() {
	printf(INFO "Testing Creation Of Tuple MV2PL\n");
	Relation<MV2PL> Relation;
	using TupleCC = Tuple<MV2PL>;
	Transaction txn1;

	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(t1);
	printf(INFO "Successfully inserted tuple at position %llu\n", pos);
}

void testCreateReadUpdateTupleMV2PL() {
	Relation<MV2PL> Relation;
	using TupleCC = Tuple<MV2PL>;
	Transaction txn1;

	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(t1);
	printf(INFO "Successfully inserted tuple at position %llu\n", pos);

	Transaction txn2;
	bool success;

	TupleCC& tup1 = Relation[pos]; // Get the original tuple
	assert(tup1.cc.IsValid(txn2) && "This is a valid update");
	success = tup1.cc.Read(txn2);
	printf(INFO "Read tuple at position %llu\n", pos);

	TupleCC tup2 = TupleCC(txn2, tup1); // Create new Tuple
	tup2.b = 3.0f; // Changes
	printf(INFO "Updated tuple at position %llu\n", pos);

	tup1.cc.UnlockRead();
	success = tup1.cc.TryLock(txn2); // Lock the tuple for no more changes
	assert(success && "This lock should succeed"); 
	printf(INFO "Locked tuple for updating %llu\n", pos);

	uint64 newPos = Relation.Insert(tup2); // Insert
	tup1.cc.Retire(txn2); // Retire the tuple
	tup1.cc.Unlock();
	printf(INFO "Unlocked and retired tuple at position %llu\n", newPos);
}

void testCreateReadUpdateTupleByMultipleTransactionMV2PL() {
	Relation<MV2PL> Relation;
	using TupleCC = Tuple<MV2PL>;

	Transaction Txn3, Txn4, Txn5;
	TupleCC _tup3 = TupleCC(Txn3, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(_tup3);

	TupleCC& tup3 = Relation[pos];

	tup3.cc.Read(Txn4);
	tup3.cc.Read(Txn5);

	tup3.cc.UnlockRead();
	bool success = tup3.cc.TryLock(Txn4);
	assert(!success && "This lock should not succeed"); 

	tup3.cc.UnlockRead();
	success = tup3.cc.TryLock(Txn5);
	assert(success && "This lock should succeed"); 

	TupleCC tup4 = TupleCC(Txn5, tup3);

	tup4.b = 3;

	Relation.Insert(tup4);
	success = tup3.cc.Retire(Txn5);
	assert(success && "This retire should succeed");

	success = tup3.cc.Retire(Txn4);
	assert(!success && "This retire should not succeed");
}

template <>
void test<MV2PL>() {
	PrintFnInfo;

	using TestFns = void (*)();
	TestFns tests[] = {
		testCreationOfTupleMV2PL,
		testCreateReadUpdateTupleMV2PL,
		testCreateReadUpdateTupleByMultipleTransactionMV2PL,
	};
	
	for (auto test : tests) {
		test();
	}
}

void testCreationOfTupleMVOCC() {
	Relation<MVOCC> Relation;
	using TupleCC = Tuple<MVOCC>;

	Transaction txn1;

	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(t1);
	printf(INFO "Successfully inserted tuple at position %llu\n", pos);
}

void testCreateReadUpdateTupleMVOCC() {
	Relation<MVOCC> Relation;
	using TupleCC = Tuple<MVOCC>;
	Transaction txn1;

	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(t1);
	printf(INFO "Successfully inserted tuple at position %llu\n", pos);
	
	// transaction 2 tries to update it
	Transaction txn2;
	TupleCC& tup1 = Relation[pos]; // Get the original tuple
	assert(tup1.cc.IsValid(txn2) && "This is a valid update");
	printf(INFO "", pos);

	TupleCC tup2 = TupleCC(txn2, tup1); // Create new Tuple
	tup2.b = 3.0f; // Changes

	bool success = tup1.cc.TryLock(txn2); // Lock the tuple for no more changes
	assert(success && "This lock should succeed"); 

	uint64 newPos = Relation.Insert(tup2); // Insert
	tup1.cc.Retire(txn2); // Retire the tuple
	tup1.cc.Unlock();

}

void testCreateReadUpdateTupleByMultipleTransactionMVOCC() {
	Relation<MVOCC> Relation;
	using TupleCC = Tuple<MVOCC>;

	Transaction Txn3, Txn4, Txn5;
	TupleCC _tup3 = TupleCC(Txn3, 1, 2.0, 3.0);
	uint64 pos = Relation.Insert(_tup3);
	printf(INFO "Successfully inserted tuple at position %llu\n", pos);

	TupleCC& tup3 = Relation[pos];
	TupleCC tup4 = TupleCC(Txn4, tup3);
	TupleCC tup5 = TupleCC(Txn5, tup3);

	tup4.b = 3;
	tup5.b = 3;

	Txn4.Update();
	Txn5.Update();

	bool success = tup3.cc.TryLock(Txn4);
	assert(success && "This lock should succeed"); 
	printf(INFO "Transaction 4 Locked tuple for updating %llu\n", pos);

	success = tup3.cc.TryLock(Txn5);
	assert(!success && "This lock should not succeed"); 
	printf(INFO "Transaction 5 Cannot lock tuple for updating %llu\n", pos);

	Relation.Insert(tup4);
	tup3.cc.Retire(Txn4);
	tup3.cc.Unlock();
}

template <>
void test<MVOCC>() {
	PrintFnInfo;
	using TestFns = void (*)();
	TestFns tests[] = {
		testCreationOfTupleMVOCC,
		testCreateReadUpdateTupleMVOCC,
		testCreateReadUpdateTupleByMultipleTransactionMVOCC,
	};

	for (auto test : tests) {
		test();
	}
}
