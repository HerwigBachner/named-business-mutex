/*
 Database synchronization Objects.

 The idea behind these classes is to use timestamps in a database to synchronize internal memory
 objects across different processes.

 Such a memory object as a key as a string and timestamp which is updated by the last change operation 
 of any process If a process write the data of the object it shoudl also update the timestamp in the database and
 inside his local sync object.

 If a second process need the data of the object and the data is already in the memory, the process should
 read the sync object timestamp. if the timestamp is newer then the local one then the data has to be
 updated.

 Because database operations are expensive in terms of performance, there is also a group support, which means
 that the timestamps of many objects are read at once.

 To initialize such a sync object at least two queries are needed
 1.Query  is an update query to update the timestamp -- used by updateDBObjectRemoteStatus
   EXAMPLES:
		update [table] set [timestamp]=now() where [key]=?



 2.Query is a read query to get the timestamp -- readDBObjectRemoteStatus
   EXAMPLES:
      select [timestamp] from [table] where [key]=?


 3.Query optional is a read query for multiple timestamps -- used by readDBGroupRemoteStatus

	EXAMPLES:
		select [key],[timestamp] from [table] where [groupkey]=?








*/
#ifndef HB_TOOLS_SYNCH_DB_OBJECTS
#define HB_TOOLS_SYNCH_DB_OBJECTS
#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <tuple>

#include <boost/date_time.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <atldbcli.h>

//#define  _SQLNCLI_OLEDB_
#ifdef _SQLNCLI_OLEDB_
#include "sqlncli.h"
#define HB_NBDBSOURCE "SQLNCLI11"
#else
#define HB_NBDBSOURCE "SQLOLEDB" 
#endif



namespace hb {
	using namespace std;
	using namespace boost::posix_time;


	class Sync_Object {
		ptime m_remote;
		ptime m_local;
		wstring m_key;
	public:	
		void remoteAndLocalTime(ptime& t) { m_local= m_remote = t; }
		void localTime(ptime& t) { m_local = t; }
		void remoteTime(ptime &t) { m_remote = t; }
		const wstring& key()const { return m_key; }
		Sync_Object(const wstring key=L""):m_key(key){}
		bool isValid()const { return m_remote <= m_local; }
		void reset_to_now(){
			m_remote = m_local = microsec_clock::universal_time();
		}
	};


	class Sync_ObjectDB : public Sync_Object {
		wstring m_querykey;
	public:
		Sync_ObjectDB(const wstring key=L"",const wstring &querykey=L""):Sync_Object(key),m_querykey(querykey){}
		void queryKey(const wstring& name) { m_querykey = name; }
		const wstring& queryKey()const { return m_querykey; }

	};


	class SyncDBGroupAccessor {
	public:
		WCHAR key[256]; DBTIMESTAMP remotets; WCHAR groupkey[256];
		BEGIN_COLUMN_MAP(SyncDBGroupAccessor)
			COLUMN_ENTRY(1, key) COLUMN_ENTRY(2, remotets) 
		END_COLUMN_MAP()
		BEGIN_PARAM_MAP(SyncDBGroupAccessor)
			SET_PARAM_TYPE(DBPARAMIO_INPUT)
			COLUMN_ENTRY(1, groupkey)
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
	};
	class SyncDBAccessor {
	public:
		WCHAR key[256]; DBTIMESTAMP remotets; 
		BEGIN_COLUMN_MAP(SyncDBAccessor)
			COLUMN_ENTRY(1, remotets)
		END_COLUMN_MAP()
		BEGIN_PARAM_MAP(SyncDBAccessor)
			SET_PARAM_TYPE(DBPARAMIO_INPUT)
			COLUMN_ENTRY(1, key) 
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
	};
	class SyncDBProcAccessor {
	public:
		WCHAR key[256]; DBTIMESTAMP remotets;
		BEGIN_PARAM_MAP(SyncDBProcAccessor)
			SET_PARAM_TYPE(DBPARAMIO_OUTPUT)
			COLUMN_ENTRY(1, remotets)
			SET_PARAM_TYPE(DBPARAMIO_INPUT)
			COLUMN_ENTRY(2, key)
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
	};

	class SyncDBConnection {
	public:
		CDataSource m_ds;
		CSession m_session;
		bool m_isopen;
		HRESULT m_hr;
		wstring m_server, m_database, m_user, m_password;

	public:
		void convertDBTIMESTAMPToPosixTime(const DBTIMESTAMP& source, boost::posix_time::ptime& dest) {
			try { // der Standard bei der fraction ist microsec (1000000 ticks pro sekunde) daher ist fractioncont 1000 wenn man datetime beim SQL Server benutzt Millisekunden
				if (source.year == 0) { dest = boost::posix_time::ptime(boost::gregorian::date(1900, 1, 1)); return; }
				dest = boost::posix_time::ptime(boost::gregorian::date(source.year, source.month, source.day),
					boost::posix_time::time_duration(source.hour, source.minute, source.second, (int)(source.fraction / 1000)));
			} catch (std::exception ex) {
				if (dest.is_not_a_date_time())
					dest = boost::posix_time::microsec_clock::universal_time();
			}
		}
	public:
		SyncDBConnection() :m_isopen(false) {}
		bool close() {
			if (m_isopen) {
				m_session.Close();
				m_ds.Close();
				m_isopen = false;
			} else return false;
			return true;
		}
		bool reopen() {
			close();
			return open();
		}
		bool is_open() const { return m_isopen; }

		bool open(const wstring& server, const wstring& database, const wstring &user,
			const wstring &password) {
			m_server = server;
			m_database = database;
			m_user = user;
			m_password = password;
			return open();
		}
		bool open(){
			CDBPropSet ps(DBPROPSET_DBINIT);

			if (m_password.empty()) {
				ps.AddProperty(DBPROP_AUTH_INTEGRATED, L"SSPI");
			} else {
				ps.AddProperty(DBPROP_AUTH_PERSIST_SENSITIVE_AUTHINFO, true);
				ps.AddProperty(DBPROP_AUTH_USERID, m_user.c_str());
				ps.AddProperty(DBPROP_AUTH_PASSWORD, m_password.c_str());
			}
			ps.AddProperty(DBPROP_INIT_DATASOURCE, m_server.c_str());
			ps.AddProperty(DBPROP_INIT_CATALOG, m_database.c_str());
			ps.AddProperty(DBPROP_INIT_PROMPT, (short)4);
			if (!FAILED(m_hr = m_ds.Open(HB_NBDBSOURCE, &ps))) {
				if (!FAILED(m_hr = m_session.Open(m_ds))) {
					m_isopen = true;
					return true;
				}
				m_ds.Close();
			}
			return false;
		}
		ptime readprocupdate(const wstring &key, const wstring& query) {
			CCommand<CAccessor<SyncDBAccessor>,CRowset,CMultipleResults> rs; rs.ClearRecord();
			HRESULT hr; DBPARAMS params;
			ptime result;
			if (FAILED(hr = rs.Create(m_session,query.c_str(), DBGUID_DEFAULT)))return result;
			if (FAILED(hr = rs.BindParameters(&rs.m_hParameterAccessor, rs.m_spCommand, &params.pData)))return result;
			params.cParamSets = 1;	params.hAccessor = rs.m_hParameterAccessor;
			wcscpy_s(rs.key, sizeof(rs.key) / sizeof(WCHAR), key.c_str());
			DBROWCOUNT rows;
			if ((hr = rs.Open(NULL, &rows,false)) == S_OK) {
				while ((hr = rs.GetNextResult(&rows, true)) == S_OK) {
					if (rs.m_spRowset) {
						//if ((hr = rs.Execute(rs.GetInterfacePtr(), &params, nullptr, &rows)) == S_OK) {
						if ((hr = rs.MoveNext()) == S_OK) {
							convertDBTIMESTAMPToPosixTime(rs.remotets, result);
						}
					}
				}
				rs.Close();
			}
			return result;
		}
		ptime readupdate(const wstring &key,const wstring& query) {
			if (boost::algorithm::icontains(query, L"execute")) {
				return readprocupdate(key, query);
			}
			CCommand<CAccessor<SyncDBAccessor>> rs; rs.ClearRecord();
			HRESULT hr; 
			ptime result;
			wcscpy_s(rs.key, sizeof(rs.key) / sizeof(WCHAR), key.c_str());
			DBROWCOUNT rows = 0;
			if ((hr = rs.Open(m_session, query.c_str())) == S_OK) {
				if ((hr = rs.MoveNext()) == S_OK) {
					convertDBTIMESTAMPToPosixTime(rs.remotets, result);
				}
				rs.Close();
			}
			return result;
		}
		vector<tuple<wstring, ptime>>readGroup(const wstring &groupkey, const wstring &query) {
			CCommand<CAccessor<SyncDBGroupAccessor>> rs; rs.ClearRecord();
			HRESULT hr; DBPARAMS params;
			vector<tuple<wstring,ptime>> result;
			wcscpy_s(rs.groupkey, sizeof(rs.groupkey) / sizeof(WCHAR), groupkey.c_str());
			if ((hr = rs.Open(m_session, query.c_str())) == S_OK) {
				while ((hr = rs.MoveNext()) == S_OK) {
					ptime rt; wstring key = rs.key;
					convertDBTIMESTAMPToPosixTime(rs.remotets, rt);
					result.push_back(make_tuple(key, rt));
				}
				rs.Close();
			}
			return result;
		}

	};

	template<typename T> 
	class SyncMap:public unordered_map<wstring, T*> {
	public:
		~SyncMap() {
			for (auto it = this->begin(); it != this->end(); ++it)delete it->second;
		}
		T* get(const wstring& key) {
			auto it = find(key);
			return (it != end()) ? it->second : nullptr;
		}
	};


	template<typename T>
	class SyncObjectStorageBase {
	protected:
		SyncMap<T> m_global_list;
	public:

		bool isObjectValid(const wstring& key) {
			T* result=m_global_list.get(key);
			return result? result->isValid() : false;
		}
		size_t collectionSize() {
			return m_global_list.size();
		}
		T* getObject(const wstring& key, bool* isnew=nullptr) {
			T* result = m_global_list.get(key);
			if (!result && isnew && *isnew) {
				result = new T(key);
				result->reset_to_now();
				m_global_list.insert(make_pair(key, result));
			} else if (isnew) *isnew = false;
			return result;
		}
	};


	class SyncObjectsStorage:public SyncObjectStorageBase<Sync_ObjectDB> {
		wstring m_server, m_database, m_user, m_password;
		unordered_map<wstring,tuple<wstring,wstring,wstring>>m_queries; 
		// query update returns one remotetimestamp has one parameter (key-string)
		// query retad returns one remotetimestamp has one parameter (key-string)
		// query group return resultset with 2 values (key-string and
		//		remotetimestamp-datetime)and has one parameter groupkey=string
		SyncDBConnection m_con;
		tuple<wstring, wstring, wstring>* getQueries(const wstring &key) {
			auto it = m_queries.find(key);
			return it != m_queries.end() ? &it->second : nullptr;
		}
		SyncDBConnection& getSQLConnection() {
			if (m_con.is_open())return m_con;
			m_con.open(m_server, m_database, m_user, m_password);
			return m_con;
		}
	public:
		void initialize(const wstring& server, const wstring& database, const wstring &user,
			const wstring &password, bool initAtl=true) {
			if(initAtl)CoInitializeEx(nullptr, COINIT_MULTITHREADED);
			m_server = server;
			m_database = database;
			m_user = user;
			m_password = password;
		}
		void addQueryBlock(const wstring& querykey,const wstring& updatequery,
			const wstring& readquery, const wstring& groupquery=L"") {
			m_queries.insert(make_pair(querykey, make_tuple(updatequery, readquery, groupquery)));
		}
		const Sync_ObjectDB& syncObject(const  wstring& key, const wstring& querykey=L"") {
			bool isnew = querykey.empty() ? false: true;
			Sync_ObjectDB* result = getObject(key,&isnew);
			if (isnew && result) result->queryKey(querykey);
			return *result;
		}
		bool readDBObjectRemoteStatus(const wstring& key) {
			Sync_ObjectDB* result = getObject(key);
			if (result) {
				SyncDBConnection &con = getSQLConnection();
				bool r = con.is_open();
				auto queries = getQueries(result->queryKey());
				if (r && queries) {
					ptime rt = con.readupdate(key, get<1>(*queries));
					if (rt.is_not_a_date_time()) r = false;
					else result->remoteTime(rt);
				}
				return r;
			}
			return false;
		}
		bool updateDBObjectRemoteStatus(const wstring& key) {
			Sync_ObjectDB* result = getObject(key);
			if (result) {
				SyncDBConnection &con = getSQLConnection();
				bool r = con.is_open();
				auto queries = getQueries(result->queryKey());
				if (r && queries) {
					ptime rt = con.readupdate(key, get<0>(*queries));
					if (rt.is_not_a_date_time()) r = false;
					else result->remoteAndLocalTime(rt);
				}
				return r;
			}
			return false;
		}
		vector<tuple<wstring,bool>> readDBGroupRemoteStatus(const wstring& groupkey, const wstring& querykey) {
			vector<tuple<wstring, bool>> result;
			SyncDBConnection &con = getSQLConnection();
			bool r = con.is_open();
			auto queries = getQueries(querykey);
			if (r && queries) {
				auto rcol = con.readGroup(groupkey, get<2>(*queries));
				for (size_t i = 0, l = rcol.size(); i < l;i++) {
					Sync_ObjectDB* obj = getObject(get<0>(rcol[i]));
					if (obj) {
						ptime rt = get<1>(rcol[i]);
						if (!rt.is_not_a_date_time()) {
							obj->remoteTime(rt);
							result.push_back(make_tuple(obj->key(), obj->isValid()));
						}
					}
				}
			}
			return result;
		}
	};



}



#endif
