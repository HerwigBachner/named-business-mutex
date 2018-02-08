
/*
Named Business Locks - Herwig Bachner (c) 2017

Named Business Locks is a library which uses locks mechanismen for business locks. The idea behind this tool set
is to have a lock which is only named and can be spanned by several threads. Each business process has it's
own business process id which is used to lock the named object. Business processes can be spanned by
several threads without any effects to the named lock. If a named business lock is locked no other
business process with a different business process id can lock the lock object.


*/




#ifndef HB_TOOLS_NAMED_LOCK
#define HB_TOOLS_NAMED_LOCK


namespace hb {
	using namespace std;




	//// Named Business Locks Main Class
	template <class T>
	class Named_Business_Lock {
	private:
		bool m_is_locked;
		bool m_auto_lock;
		string m_name;
		string m_process_id;
		int m_last_error_code;
		std::shared_ptr<T> m_item;
	public:
		const string& get_name() { return m_name; }
	private:
		std::shared_ptr<T> get_lock() {
			if (m_item)return m_item;
			return T::get_lock(m_name);
		}
	public:
		Named_Business_Lock(const string &name, const string& process_id, bool auto_lock = false) :
			m_name(name), m_process_id(process_id), m_last_error_code(0), m_item(nullptr) {
			auto x = get_lock();
			m_auto_lock = auto_lock;
			m_last_error_code = x ? 0 : -99;
			m_is_locked = m_auto_lock && x ? m_last_error_code = lock() : false;
		}
		~Named_Business_Lock() {
			if (m_auto_lock && is_locked_by_me()) {
				auto x = get_lock();
				while (x && x->rescursive_lock_count() > 0 && x->unlock(m_process_id));
			}
		}
		int lastErrorCode() { return m_last_error_code; }

	public: // interface
		// locks the named_object  with a timeout for waiting 
		// After the timeout is reached the wait will be brocken if the callback_break function returns true or is not present
		// the lock function returns true if succeeded.
		bool lock(int wait_for_lock_sec = NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut(),
			std::function<bool(string&)>callback_break = nullptr,
			int time_out_sec = NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut()) {
			return (*const_cast<int*>(&m_last_error_code) = get_lock()->lock(m_process_id, wait_for_lock_sec, time_out_sec)) > 0;
		}
		// returns true if succeeded
		bool unlock() {
			return (*const_cast<int*>(&m_last_error_code) = get_lock()->unlock(m_process_id)) > -1;
		}
		// tries to get the lock on the named object without waiting - returns true if succeeded
		bool trylock(std::function<bool(string&)>callback_break = nullptr,
			int time_out_sec = NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut()) {
			return (*const_cast<int*>(&m_last_error_code) = get_lock()->try_lock(m_process_id, time_out_sec)) > 0;
		}
		// works like lock but it locks the named object only till lock_until.
		// after lock_until is reached the lock is free again.
		// lock_until is a UTC time stamp
		bool time_lock(boost::posix_time::ptime& lock_until,
			int wait_for_lock_sec = NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut(),
			std::function<bool(string&)>callback_break = nullptr) {
			m_last_error_code = get_lock()->time_lock(m_process_id, lock_until, wait_for_lock_sec);
			return m_last_error_code > 0;
		}
		bool is_locked_by_me() {
			auto x = get_lock();
			return x ? (x->rescursive_lock_count() > 0 && x->process_id() == m_process_id) : false;
		}

		int get_recursive_count() {
			auto x = get_lock();
			return x ? x->rescursive_lock_count() : 0;
		}
		bool is_locked() {
			auto x = get_lock();
			return x && x->rescursive_lock_count();
		}
		string get_lockowned_process_id() {
			auto x = get_lock();
			return x ? x->process_id() : "";
		}
		bool reset() {
			bool result = !is_locked_by_me();
			auto x = get_lock();
			if (x) x->clear();
			return result;
		}
		bool remove() {
			return false;
		}
	};

}

#endif

