#include <iostream>
#include <vector>
#include <cassert>
using namespace std;

using uint64 = unsigned long long;
using uint32 = unsigned long long;

#define atomic_add(ptr, val) __atomic_add_fetch(ptr, val, __ATOMIC_ACQ_REL)
#define atomic_fetch(ptr) __atomic_load_n(ptr, __ATOMIC_ACQUIRE)
#define atomic_store(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELEASE)
#define atomic_cas(ptr, from, to) __atomic_compare_exchange_n(ptr, from, to, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)

// Monotonic increasing transaction id
uint64 tid = 0;

struct Transaction {
	uint64 tid;

	Transaction() {
		this->tid = atomic_add(&tid, 1);
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
	void Unlock() {
		atomic_store(&this->tid, 0);
	}

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

	bool _CanWrite(const Transaction& T) {
		uint64 v = atomic_fetch(&(this->tid));
		return v == 0 || v == T.tid;
	}

	bool IsValid(const Transaction& T) {
		uint64 tid = T.tid;
		return this->begin <= tid && tid < this->end && tid >= this->read;
	}

	void Read(const Transaction& T) {
		atomic_store(&this->read, T.tid);
	}

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

	void Read(const Transaction& T) {}

	bool IsValid(const Transaction& T) {
		uint64 tid = T.tid;
		return this->begin <= tid && tid < this->end;
	}

	// NOTE: This Transaction has a new Id
	bool TryLock(const Transaction& Tcommit) {
		if (!this->IsValid(Tcommit)) return false;
		// Attempt to set new tid otherwise skip
		return atomic_cas(&this->tid, 0, Tcommit.tid); 
	}

	void Retire(const Transaction& T) {
		atomic_store(&this->end, T.tid);
	}

	bool Unlock(const Transaction& T) {
		return atomic_cas(&this->tid, T.tid, 0);
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
using TupleCC = Tuple<CC>;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
vector<TupleCC> Relation;

uint64 InsertInRelation(TupleCC t) {
	pthread_mutex_lock(&mutex);

	int pos = Relation.size();
	Relation.push_back(t);
	Relation[pos].cc.Unlock();

	pthread_mutex_unlock(&mutex);
	return pos;
}

int main() {
	pthread_mutex_init(&mutex, NULL);

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
	Relation[newPos].cc.Unlock(); // Unlock the new tuple
	Relation[pos].cc.Unlock(); // Unlock the old tuple
}

