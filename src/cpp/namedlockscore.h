

#ifndef HB_TOOLS_NAMED_LOCK_CORE
#define HB_TOOLS_NAMED_LOCK_CORE
// STD
#include <string>
#include <thread>
#include <mutex>
#include <exception>

#define BOOST_DATE_TIME_NO_LIB 1
#include <boost/date_time.hpp>
#include <boost/date_time/date_format_simple.hpp>
#include <boost/date_time/special_defs.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/gregorian/greg_month.hpp>
#include <boost/date_time/gregorian/greg_facet.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

//#define NamedBusinessDefaultTimeOut 120

namespace hb {
	using namespace std;
	// Base Class does not work standanlone!!!!!
	class NamedBusiness_Lock_Item {
	protected:
		string m_name;
		std::recursive_mutex m_lock;
		string m_process_id;
		boost::posix_time::ptime m_expire;
		int m_lock_item;
	protected:
		bool is_expired() {
			boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
			bool result;
			m_lock.lock();
			result = now > m_expire;
			m_lock.unlock();
			return result;
		}
		void set_expired(int time_out_sec) {
			boost::posix_time::ptime expire = boost::posix_time::microsec_clock::universal_time() +
				boost::posix_time::time_duration(0, 0, time_out_sec);
			set_expired(expire);
		}
		void set_expired(boost::posix_time::ptime expire) {
			m_lock.lock();
			m_expire = expire;
			m_lock.unlock();
		}

	public:
		NamedBusiness_Lock_Item(const string& name) :m_name(name), m_lock_item(0) {}
		const string &name() { return m_name; }

		static int NamedBusinessDefaultTimeOut() { return  120; }

	};



}


#endif
