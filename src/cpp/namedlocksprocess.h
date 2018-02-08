

#ifndef HB_TOOLS_NAMED_LOCK_PROCESS
#define HB_TOOLS_NAMED_LOCK_PROCESS

#include  <boost/date_time.hpp>

#include <unordered_map>


#include "namedlockscore.h"


namespace hb {
	using namespace std;

	class NamedBusiness_Lock_ProcessItem :public NamedBusiness_Lock_Item {
	private:
	private:
		const string get_process_id() {
			string result;
			m_lock.lock();
			result = m_process_id;
			m_lock.unlock();
			return result;
		}
		void set_process_id(const string& process_id) {
			m_lock.lock();
			m_process_id = process_id;
			m_lock.unlock();
		}
		bool is_foreign_locked(const string& process_id) {
			return m_lock_item > 0 && !is_expired() && process_id != get_process_id();
		}
		bool increment(const string& process_id, int time_out_sec) {
			bool result = false;
			m_lock.lock();
			if ((m_lock_item <= 0) || is_expired()) {
				m_lock_item = 1;
				m_process_id = process_id;
				set_expired(time_out_sec);
				result = true;
			} else if (m_process_id == process_id) {
				m_lock_item++;
				set_expired(time_out_sec);
				result = true;
			}
			m_lock.unlock();
			return result;
		}
		bool decrement(const string& process_id) {
			bool result = false;
			m_lock.lock();
			if (m_process_id == process_id) {
				if (m_lock_item > 0) m_lock_item--;
				if (m_lock_item == 0) m_process_id.clear();
				result = true;
			}
			m_lock.unlock();
			return result;
		}
	public:
		NamedBusiness_Lock_ProcessItem(const string& name) :NamedBusiness_Lock_Item(name) {}
		int lock(const string process_id, int wait_for_lock_sec = 
			NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut(),
			int time_out_sec = NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut()) {
			if (!this) return -1;
			for (int t = 0, tout = wait_for_lock_sec * 1000; t < tout; t += 10) {
				if (try_lock(process_id, time_out_sec) > 0)
					return m_lock_item;
				::Sleep(10);
			}
			return -1;
		}
		int unlock(const string process_id) {
			if (!this) return -1;
			return decrement(process_id) ? m_lock_item : -1;
		}
		int try_lock(const string process_id, int time_out_sec = 
			NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut()) {
			if (!this) return -1;
			return increment(process_id, time_out_sec) ? m_lock_item : -1;
		}
		int time_lock(const string process_id, boost::posix_time::ptime timeout,
			int wait_for_lock_sec = NamedBusiness_Lock_Item::NamedBusinessDefaultTimeOut()) {
			int result = lock(process_id, wait_for_lock_sec);
			if (result >= 0)set_expired(timeout);
			return result;
		}
		int rescursive_lock_count() { return this ? m_lock_item : -1; }
		const string process_id() { return get_process_id(); }
		void clear() {
			m_lock_item = 0;
		}
		static std::shared_ptr<NamedBusiness_Lock_ProcessItem> get_lock(const string  name);
	};


	class Named_Business_Lock_List {
	private:
		unordered_map<string, std::shared_ptr<NamedBusiness_Lock_ProcessItem>> m_list;
		std::recursive_mutex m_lock;
	private:
		std::shared_ptr<NamedBusiness_Lock_ProcessItem> find(const string& name) {
			auto &it = m_list.find(name);
			return (it != m_list.end()) ? it->second : nullptr;
		}
	public:
		std::shared_ptr<NamedBusiness_Lock_ProcessItem> get_insert_lock(const string& name) {
			std::shared_ptr<NamedBusiness_Lock_ProcessItem> result ;
			if (!this) return result;
			m_lock.lock();
			try {
				if ((result = find(name)) == nullptr) {
					m_list.insert(make_pair(name, std::shared_ptr<NamedBusiness_Lock_ProcessItem>(
						new NamedBusiness_Lock_ProcessItem(name))));
					result = find(name);
				}
			} catch (...) {
			}
			m_lock.unlock();
			return result;
		}
		const std::shared_ptr<NamedBusiness_Lock_ProcessItem> get_lock(const string& name) {
			std::shared_ptr<NamedBusiness_Lock_ProcessItem> result;
			if (!this) return result;
			m_lock.lock();
			try {
				result = find(name);
			} catch (...) {

			}
			m_lock.unlock();
			return result;
		}
		static Named_Business_Lock_List& getGlobalList() {
			static Named_Business_Lock_List bussiness_lock_list;
			return bussiness_lock_list;
		}
	};


	 std::shared_ptr<NamedBusiness_Lock_ProcessItem> NamedBusiness_Lock_ProcessItem::get_lock(const string  name) {
		return Named_Business_Lock_List::getGlobalList().get_insert_lock(name);
	}


}


#endif
