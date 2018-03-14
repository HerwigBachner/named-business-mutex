
#ifndef HB_TOOLS_SYNCH_DB_OBJECTS
#define HB_TOOLS_SYNCH_DB_OBJECTS
#include <string>
#include <memory>
#include <unordered_map>

#include <boost/date_time.hpp>
#include <atldbcli.h>

#ifdef _SQLNCLI_OLEDB_
#include "sqlncli.h"
#define HB_NBDBSOURCE "SQLNCLI11"
#else
#define HB_NBDBSOURCE "SQLOLEDB" 
#endif

namespace hb {
	using namespace std;
	using namespace boost::posix_time;

	struct Sync_Objects_Connection_Names {
		wstring m_server, m_database, m_user, m_password;
		void initialize(const wstring& server, const wstring& database, const wstring &user,
			const wstring &password) {
			m_server = server;
			m_database = database;
			m_user = user;
			m_password = password;
		}
	};

	struct Sync_Object {
		ptime m_last_sync;
		ptime m_last_db_read;
		wstring m_key;

		
		bool is_valid() { return m_last_db_read <= m_last_sync; }
		Sync_Object* reset_to_now(){
			m_last_db_read = m_last_sync = microsec_clock::universal_time();
			return this;
		}
	};


	class Sync_Map :unordered_map<wstring, *Sync_Object> {
	public:
		Sync_Object* get(const wstring &key){
			Sync_Object* result = nullptr; 
			auto &it = find(key);
			if (it == end()){
				insert(result = new Sync_Object()->reset_to_now());
			} else result = it->second;
			return result;
		}
		~Sync_Map() {
			for (auto &it = begin(); it != end(); ++it;)
				delete it->second;
		}

	};

}



#endif
