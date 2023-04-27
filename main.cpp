#include <iostream>
#include <vector>
#include <cassert>
using namespace std;

using uint64 = unsigned long long;
using uint32 = unsigned long long;

#define UNIMPLEMENTED assert(0 && "Unimplemented")
#define atomic_add(ptr, val) __atomic_add_fetch(ptr, val, __ATOMIC_ACQ_REL)
#define atomic_fetch(ptr) __atomic_load_n(ptr, __ATOMIC_ACQUIRE)
#define atomic_store(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELEASE)
#define atomic_cas(ptr, from, to) __atomic_compare_exchange_n(ptr, from, to, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)

// Monotonic increasing transaction id
uint64 GlobalTid = 0;

struct Transaction {
	uint64 tid;

	Transaction() {
		this->tid = atomic_add(&GlobalTid, 1);
	}

	void Update() {
		this->tid = atomic_add(&GlobalTid, 1);
	}
};

// Multi Version Timestamp Ordering
struct MVTO {
	// This contains additional read timestamp.
	// Everytime a tuple is read it sets the read timestamp
	// to the current transaction id if its value is less than 
	// the current transaction id.
	//
	// For transaction T that creates a new tuple B(x+1) if the original has
	// 1) no active transaction holding B(x) and
	// 2) Transaction id is larger than B(x).read
	//
	// When B(x+1) commit it sets B(x).begin = T.tid and B(x).end = (uint64)-1

	uint64 tid;
	uint64 read;
	uint64 begin;
	uint64 end;

	MVTO() {}

	MVTO(const Transaction& Txn) {
		uint64 tid = Txn.tid;

		this->tid = tid;
		this->read = tid;
		this->begin = tid;
		this->end = (uint64)-1;
	}

	// XXX: Ensure can only be called by the thread that locked it
	// Access is allowed
	void Unlock() {
		// Note: can only be called after Lock()
		atomic_store(&this->tid, 0);
	}

	// Restricting Access
	bool TryLock(const Transaction& T) {
		uint64 tid = T.tid;
		uint64 zero = 0;
		if (atomic_cas(&(this->tid), &zero, tid)) {
			// Locked
			if (atomic_fetch(&this->read) > tid) {	
				atomic_store(&this->tid, 0);
				return false;
			}

			atomic_store(&this->tid, tid);
			return true;
		}

		// Failed To Lock
		return false;
	}

	// Tuple valid for the transaction
	bool IsValid(const Transaction& T) {
		uint64 tid = T.tid;
		return this->begin <= tid && tid < this->end && tid >= this->read;
	}

	// Notify the tuple that it is being read
	bool Read(const Transaction& T) {
		atomic_store(&this->read, T.tid);
		return true;
	}

	// Tuple holds stale value upto end
	void Retire(const Transaction& T) { 
		atomic_store(&this->end, T.tid);
	}
};

// Multi Version Optimistic Concurrecy Control
struct MVOCC {
	// MVOCC Splits the transactions in three phases :
	// 1) Read Phase: Transaction reads the tuple and updates the value in database
	// 	- The read is done when the transaction is between the begin and end
	// 	- The tuple is unlocked
	// 	- If the transaction creates new tuple it sets the begin to the transaction id
	// 2) Validation Phase: Transaction want to commit the changes
	//  - Another timestamp is assigned to the transaction (T_commit) to determine the serialization order of the transaction. 
	//  - Check if the tuples were already updated by another transaction
	//
	// 3) Write Phase: Transaction enters the writer Phase

	uint64 tid;
	uint64 begin;
	uint64 end;

	MVOCC() {}

	MVOCC(const Transaction& Txn) {
		uint64 tid = Txn.tid;

		this->tid = tid;
		this->begin = tid;
		this->end = (uint64)-1;
	}

	bool Read(const Transaction& T) {
		return true;
	}

	bool IsValid(const Transaction& T) {
		uint64 tid = T.tid;
		return this->begin <= tid && tid < this->end;
	}

	// NOTE: This Transaction has a new Id
	bool TryLock(const Transaction& Tcommit) {
		// Locking is done only if the transaction id is between
		// start and end and the lock isn't held by currenly running
		// transaction
		if (!this->IsValid(Tcommit)) return false;

		// Attempt to set new tid otherwise skip
		uint64 zero = 0;
		return atomic_cas(&this->tid, &zero, Tcommit.tid); 
	}

	void Retire(const Transaction& T) {
		atomic_store(&this->end, T.tid);
	}

	void Unlock() {
		atomic_store(&this->tid, 0);
	}
};

struct MV2PL {
	// To perform a read operation on a tuple A, 
	// the DBMS searches for a visible version by comparing a transaction’s Tid with the tuples’ begin-ts field. 
	// If it finds a valid version, 
	// 		then the DBMS increments that tuple’s read-cnt field 
	// 			if its txn-id field is equal to zero 
	// 	Similarly, a transaction is allowed to update a version Bx 
	// 		if both read-cnt and txn-id are set to zero. 
	// 	When a transaction commits, 
	// 		the DBMS assigns it a unique timestamp (Tcommit) 
	// 		that is used to update the begin-ts field for the versions created by that transaction and then releases all of the transaction’s locks.

	uint64 tid;
	uint64 begin;
	uint64 end;
	uint64 readCount;

	MV2PL() {}

	MV2PL(const Transaction& Txn) {
		uint64 tid = Txn.tid;

		this->tid = tid;
		this->readCount = 0;
		this->begin = tid;
		this->end = (uint64)-1;
	}

	void Unlock() {
		atomic_store(&this->tid, 0);
	}

	bool TryLock(const Transaction& T) {
		if (atomic_fetch(&this->readCount) != 0) return false;
		if (atomic_fetch(&this->tid) != 0) return false;

		uint64 zero = 0;
		bool success = atomic_cas(&this->tid, &zero, T.tid);
		if (!success) return false;

		if (atomic_fetch(&this->readCount) != 0) {
			atomic_store(&this->tid, 0);
			return false;
		}

		return true;
	}

	bool Read(const Transaction& T) {
		if (atomic_fetch(&this->readCount) != 0) return false;
		atomic_add(&this->readCount, 1);
	}

	bool IsValid(const Transaction& T) {
		uint64 tid = T.tid;
		return this->begin <= tid && tid < this->end;
	}

};


// The Tuple 
template <typename ConcurrencyControl>
struct Tuple {
	ConcurrencyControl cc;

	// Data
	int a;
	float b;
	double c;

	Tuple() {}
	Tuple(const Transaction& T, int a, float b, double c) {
		this->a = a;
		this->b = b;
		this->c = c;
		this->cc = ConcurrencyControl(T);
	}

	Tuple(const Transaction& T, const Tuple &d) {
		this->a = d.a;
		this->b = d.b;
		this->c = d.c;
		this->cc = ConcurrencyControl(T);
	}
};

using CC = MVTO;
// using CC = MVOCC;
using TupleCC = Tuple<CC>;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
vector<TupleCC> Relation;

// This function call unlocks the tuple 
// no need to call it at the moment
uint64 InsertInRelation(TupleCC& t) {
	pthread_mutex_lock(&mutex);

	int pos = Relation.size();
	Relation.push_back(t);
	Relation[pos].cc.Unlock();

	pthread_mutex_unlock(&mutex);
	return pos;
}

template <typename T> 
void test() {}

template <> 
void test<MVTO>() {
	pthread_mutex_init(&mutex, NULL);
	Relation.reserve(100);

	Transaction txn1;

	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = InsertInRelation(t1);
	
	// transaction 2 tries to update it
	Transaction txn2;
	TupleCC& tup1 = Relation[pos]; // Get the original tuple
	assert(tup1.cc.IsValid(txn2) && "This is a valid update");

	TupleCC tup2 = TupleCC(txn2, tup1); // Create new Tuple
	tup2.b = 3.0f; // Changes

	bool success = tup1.cc.TryLock(txn2); // Lock the tuple for no more changes
	assert(success && "This lock should succeed"); 

	uint64 newPos = InsertInRelation(tup2); // Insert
	tup1.cc.Retire(txn2); // Retire the tuple
	tup1.cc.Unlock();
												
	// Testing for overlaps in writing
	Transaction Txn3, Txn4, Txn5;
	TupleCC _tup3 = TupleCC(Txn3, 1, 2.0, 3.0);
	pos = InsertInRelation(_tup3);

	TupleCC& tup3 = Relation[pos];
	tup3.cc.Read(Txn4);
	tup3.cc.Read(Txn5);

	success = tup3.cc.TryLock(Txn4);
	assert(!success && "This lock should not succeed"); 

	success = tup3.cc.TryLock(Txn5);
	assert(success && "This lock should succeed"); 

	TupleCC tup4 = TupleCC(Txn4, tup3);

	tup4.b = 3;

	InsertInRelation(tup4);
	tup3.cc.Retire(Txn4);
}

template <>
void test<MVOCC>() {
	pthread_mutex_init(&mutex, NULL);
	Relation.reserve(100);

	Transaction txn1;

	TupleCC t1 = TupleCC(txn1, 1, 2.0, 3.0);
	uint64 pos = InsertInRelation(t1);
	
	// transaction 2 tries to update it
	Transaction txn2;
	TupleCC& tup1 = Relation[pos]; // Get the original tuple
	assert(tup1.cc.IsValid(txn2) && "This is a valid update");

	TupleCC tup2 = TupleCC(txn2, tup1); // Create new Tuple
	tup2.b = 3.0f; // Changes

	bool success = tup1.cc.TryLock(txn2); // Lock the tuple for no more changes
	assert(success && "This lock should succeed"); 

	uint64 newPos = InsertInRelation(tup2); // Insert
	tup1.cc.Retire(txn2); // Retire the tuple
	tup1.cc.Unlock();
												
	// Testing for overlaps in writing
	Transaction Txn3, Txn4, Txn5;
	TupleCC _tup3 = TupleCC(Txn3, 1, 2.0, 3.0);
	pos = InsertInRelation(_tup3);

	TupleCC& tup3 = Relation[pos];
	TupleCC tup4 = TupleCC(Txn4, tup3);
	TupleCC tup5 = TupleCC(Txn5, tup3);

	tup4.b = 3;
	tup5.b = 3;

	Txn4.Update();
	Txn5.Update();

	success = tup3.cc.TryLock(Txn4);
	assert(success && "This lock should succeed"); 

	success = tup3.cc.TryLock(Txn5);
	assert(!success && "This lock should not succeed"); 

	InsertInRelation(tup4);
	tup3.cc.Retire(Txn4);
	tup3.cc.Unlock();
}

int main() {
	test<CC>();
}

