

#ifndef HB_TOOLS_NAMED_LOCK_DATABASE
#define HB_TOOLS_NAMED_LOCK_DATABASE
#include <atldbcli.h>

#ifdef _SQLNCLI_OLEDB_
#include "sqlncli.h"
#define HB_NBDBSOURCE "SQLNCLI11"
#else
#define HB_NBDBSOURCE "SQLOLEDB" 
#endif

#include "namedlockscore.h"

namespace hb {
	using namespace std;

	struct NamedBusiness_Lock_Connection_Names {
		wstring m_server, m_database, m_user, m_password;
		void initialize(const wstring& server, const wstring& database, const wstring &user,
			const wstring &password) {
			m_server = server;
			m_database = database;
			m_user = user;
			m_password = password;
		}
	};

	class NamedBusiness_Lock_Connection {
	private:
		CDataSource m_ds;
		CSession m_session;
		bool m_isopen;
		const wchar_t* ms_create_statement = L"\
					IF object_id('named_mutex') is null \
			BEGIN\
			CREATE TABLE[named_mutex]([name] varchar(256) NOT NULL, [processid] varchar(256),\
				[lockcount] int, [timeout] datetime2\
				INDEX[named_mutex_ix1] NONCLUSTERED([name]));\
			END";
	public:
		HRESULT m_hr;
		static NamedBusiness_Lock_Connection_Names& getGlobalConnectionNames() {
			static NamedBusiness_Lock_Connection_Names global_connection_names;
			return global_connection_names;
		}
	public:
		bool execute_command(const wstring &command, DBROWCOUNT *rowseffected = nullptr) {
			CCommand<CNoAccessor, CNoRowset> x;
			bool ret = !FAILED(m_hr = x.Open(m_session, command.c_str(), nullptr, rowseffected));
			return ret;
		}
		HRESULT get_last_hresult() { return m_hr; }
		CSession& get_session() { return m_session; }
	public:
		NamedBusiness_Lock_Connection() :m_isopen(false) {}
		~NamedBusiness_Lock_Connection() {
			close();
		}
		bool is_open() const { return m_isopen; }
		bool open(bool check_create_table = false) {
			CDBPropSet ps(DBPROPSET_DBINIT);

			auto names = NamedBusiness_Lock_Connection::getGlobalConnectionNames();
			if (names.m_password.size() == 0) {
				ps.AddProperty(DBPROP_AUTH_INTEGRATED, L"SSPI");
			} else {
				ps.AddProperty(DBPROP_AUTH_PERSIST_SENSITIVE_AUTHINFO, true);
				ps.AddProperty(DBPROP_AUTH_USERID, names.m_user.c_str());
				ps.AddProperty(DBPROP_AUTH_PASSWORD, names.m_password.c_str());
			}
			ps.AddProperty(DBPROP_INIT_DATASOURCE, names.m_server.c_str());
			ps.AddProperty(DBPROP_INIT_CATALOG, names.m_database.c_str());
			ps.AddProperty(DBPROP_INIT_PROMPT, (short)4);
			if (!FAILED(m_hr = m_ds.Open(HB_NBDBSOURCE, &ps))) {
				if (!FAILED(m_hr = m_session.Open(m_ds))) {
					m_isopen = true;
					return check_create_table ? execute_command(ms_create_statement) : true;
				}
				m_ds.Close();
			}
			return false;
		}
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
		bool check_valid_database() {
			if (m_isopen)
				return execute_command(ms_create_statement);
			return false;
		}
	};
	__forceinline bool Initialize_NamedBusinessLocks_For_Database(const wstring& server,
		const wstring& database, const wstring &user, const wstring &password, bool check_database_exists = false) {
		CoInitializeEx(nullptr, COINIT_MULTITHREADED);
		NamedBusiness_Lock_Connection::getGlobalConnectionNames().initialize(server, database, user, password);
		NamedBusiness_Lock_Connection connection;
		bool ret = connection.open(check_database_exists);
		return ret;
	}


	// Process internals list



	class NamedBusiness_Lock_Database_ReadAccessor {
	public:
		CHAR processid[256]; INT lockcount; DBTIMESTAMP timeout; CHAR name[256];
		BEGIN_COLUMN_MAP(NamedBusiness_Lock_Database_ReadAccessor)
			COLUMN_ENTRY(1, processid) COLUMN_ENTRY(2, lockcount) COLUMN_ENTRY(3, timeout)
		END_COLUMN_MAP()
		BEGIN_PARAM_MAP(NamedBusiness_Lock_Database_ReadAccessor)
			SET_PARAM_TYPE(DBPARAMIO_INPUT)
			COLUMN_ENTRY(1, name)
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
		wstring GetSCommand() {
			return  L"select processid,lockcount,timeout from named_mutex where name=?";
		}
	};
	class NamedBusiness_Lock_Database_InsertAccessor {
	public:
		CHAR processid[256]; INT lockcount; DBTIMESTAMP timeout; CHAR name[256];
		BEGIN_PARAM_MAP(NamedBusiness_Lock_Database_InsertAccessor)
			COLUMN_ENTRY(1, name) 	COLUMN_ENTRY(2, processid)
			COLUMN_ENTRY(3, lockcount) COLUMN_ENTRY(4, timeout)
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
		wstring GetSCommand() {
			return L"insert into named_mutex(name,processid,lockcount,timeout) values(?,?,?,?) ";
		}
	};
	class NamedBusiness_Lock_Database_IncrementAccessor {
	public:
		CHAR processid[256]; INT lockcount; DBTIMESTAMP timeout; CHAR name[256];
		BEGIN_PARAM_MAP(NamedBusiness_Lock_Database_IncrementAccessor)
			COLUMN_ENTRY(1, processid)
			COLUMN_ENTRY(2, lockcount) COLUMN_ENTRY(3, lockcount) COLUMN_ENTRY(4, timeout)
			COLUMN_ENTRY(5, name) 	COLUMN_ENTRY(6, processid)
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
		wstring GetSCommand() {
			// ((lockcount<=0)|| (dbprocessid==processid) || (dbtimestamp < now)) ? set : noset
			return L"update named_mutex set processid=?, lockcount=\
			IIF((lockcount < ?) or (timeout< GETUTCDATE()),?,lockcount+1), timeout=? \
			where name=? and (\
			(lockcount <= 0) or (processid=?) or (timeout< GETUTCDATE())) ";
		}
	};
	class NamedBusiness_Lock_Database_DecrementAccessor {
	public:
		CHAR processid[256];  CHAR name[256];
		BEGIN_PARAM_MAP(NamedBusiness_Lock_Database_DecrementAccessor)
			COLUMN_ENTRY(1, name) 	COLUMN_ENTRY(2, processid)
		END_PARAM_MAP()
		void ClearRecord() { memset(this, 0, sizeof(*this)); }
		wstring GetSCommand() {
			return L"update named_mutex set lockcount=IIF(lockcount <= 0,0,lockcount-1),\
			processid=IIF(lockcount=1,'',processid) where name=? and \
			(lockcount > 0) and (processid=?) ";
		}
	};
	__forceinline void ConvertPosixTimeToDBTIMESTAMP2(const boost::posix_time::ptime& source, DBTIMESTAMP& dest) {
		dest.year = source.date().year();
		dest.month = source.date().month();
		dest.day = source.date().day();
		dest.hour = source.time_of_day().hours();
		dest.minute = source.time_of_day().minutes();
		dest.second = source.time_of_day().seconds();
		int fr = (int)source.time_of_day().fractional_seconds(); // mikrosekunden for datetime2
		fr *= 1000; // nanosekunden
		dest.fraction = (ULONG)(fr);
	}
	__forceinline void ConvertDBTIMESTAMPToPosixTime(const DBTIMESTAMP& source, boost::posix_time::ptime& dest) {
		try { // der Standard bei der fraction ist microsec (1000000 ticks pro sekunde) daher ist fractioncont 1000 wenn man datetime beim SQL Server benutzt Millisekunden
			if (source.year == 0) { dest = boost::posix_time::ptime(boost::gregorian::date(1900, 1, 1)); return; }
			dest = boost::posix_time::ptime(boost::gregorian::date(source.year, source.month, source.day),
				boost::posix_time::time_duration(source.hour, source.minute, source.second, (int)(source.fraction / 1000)));
		} catch (std::exception ex) {
			if (dest.is_not_a_date_time())
				dest = boost::posix_time::microsec_clock::universal_time();
		}
	}

	class NamedBusiness_Lock_DatabaseItem :public NamedBusiness_Lock_Item {
	private:
		NamedBusiness_Lock_Connection m_connection;

	private:
		bool check() const {
			if (!this)return false;
			if (!m_connection.is_open() && !(const_cast<NamedBusiness_Lock_Connection&>(m_connection)).open())
				return false;
			return true;
		}

		bool create_data(int time_out_sec) {
			CCommand<CAccessor<NamedBusiness_Lock_Database_InsertAccessor> > rs; rs.ClearRecord();
			HRESULT hr; DBPARAMS params;
			if (FAILED(hr = rs.Create(m_connection.get_session(), rs.GetSCommand().c_str(), DBGUID_DEFAULT)))return false;
			if (FAILED(hr = rs.BindParameters(&rs.m_hParameterAccessor, rs.m_spCommand, &params.pData)))return false;
			params.cParamSets = 1;	params.hAccessor = rs.m_hParameterAccessor;
			strcpy_s(rs.name, sizeof(rs.name) / sizeof(CHAR), name().c_str());
			strcpy_s(rs.processid, sizeof(rs.processid) / sizeof(CHAR), m_process_id.c_str());
			rs.lockcount = 0;
			ConvertPosixTimeToDBTIMESTAMP2(boost::posix_time::microsec_clock::universal_time() +
				boost::posix_time::time_duration(0, 0, time_out_sec), rs.timeout);
			if (FAILED(hr = rs.Execute(rs.GetInterfacePtr(), &params, nullptr, nullptr))) return false;
			return true;
		}

		bool read_data(bool checknew = false) {
			CCommand<CAccessor<NamedBusiness_Lock_Database_ReadAccessor>> rs; rs.ClearRecord();
			HRESULT hr;
			bool result = false;
			strcpy_s(rs.name, sizeof(rs.name) / sizeof(CHAR), name().c_str());
			if ((hr = rs.Open(m_connection.get_session(), rs.GetSCommand().c_str())) == S_OK) {
				if ((hr = rs.MoveNext()) == S_OK) {
					m_lock_item = rs.lockcount;
					m_process_id = rs.processid;
					boost::posix_time::ptime t;
					ConvertDBTIMESTAMPToPosixTime(rs.timeout, t);
					set_expired(t);
					result = true;
				} else if (checknew && (hr == DB_S_ENDOFROWSET)) {
					result = create_data(0);
				}
				rs.Close();
			}
			return result;
		}

		bool increment_data(boost::posix_time::ptime & time_out) {
			CCommand<CAccessor<NamedBusiness_Lock_Database_IncrementAccessor> > rs; rs.ClearRecord();
			HRESULT hr; DBPARAMS params;
			if (FAILED(hr = rs.Create(m_connection.get_session(), rs.GetSCommand().c_str(), DBGUID_DEFAULT)))return false;
			if (FAILED(hr = rs.BindParameters(&rs.m_hParameterAccessor, rs.m_spCommand, &params.pData)))return false;
			params.cParamSets = 1;	params.hAccessor = rs.m_hParameterAccessor;
			strcpy_s(rs.name, sizeof(rs.name) / sizeof(CHAR), name().c_str());
			strcpy_s(rs.processid, sizeof(rs.processid) / sizeof(CHAR), m_process_id.c_str());
			rs.lockcount = m_lock_item;
			ConvertPosixTimeToDBTIMESTAMP2(time_out, rs.timeout);
			DBROWCOUNT rows = 0;
			if (FAILED(hr = rs.Execute(rs.GetInterfacePtr(), &params, nullptr, &rows))) return false;
			return rows == 1;
		}

		bool increment_data(int time_out_sec) {
			return increment_data(boost::posix_time::microsec_clock::universal_time() +
				boost::posix_time::time_duration(0, 0, time_out_sec));
		}
		bool decrement_data() {
			CCommand<CAccessor<NamedBusiness_Lock_Database_DecrementAccessor> > rs; rs.ClearRecord();
			HRESULT hr; DBPARAMS params;
			if (FAILED(hr = rs.Create(m_connection.get_session(), rs.GetSCommand().c_str(), DBGUID_DEFAULT)))return false;
			if (FAILED(hr = rs.BindParameters(&rs.m_hParameterAccessor, rs.m_spCommand, &params.pData)))return false;
			params.cParamSets = 1;	params.hAccessor = rs.m_hParameterAccessor;
			strcpy_s(rs.name, sizeof(rs.name) / sizeof(CHAR), name().c_str());
			strcpy_s(rs.processid, sizeof(rs.processid) / sizeof(CHAR), m_process_id.c_str());
			DBROWCOUNT rows = 0;
			if (FAILED(hr = rs.Execute(rs.GetInterfacePtr(), &params, nullptr, &rows))) return false;
			return rows == 1;
		}

		bool increment(const string& processid, int time_out_sec) {
			bool result = false;
			m_lock.lock();
			try {
				if (read_data(true)) {
					if ((m_lock_item <= 0) || is_expired()) {
						m_lock_item = 1;
						m_process_id = processid;
						result = increment_data(time_out_sec);
					} else if (m_process_id == processid) {
						m_lock_item++;
						result = increment_data(time_out_sec);
					}
				}
			} catch (...) {}
			m_lock.unlock();
			return result;
		}
		bool increment(const string& processid, boost::posix_time::ptime &time_out) {
			bool result = false;
			m_lock.lock();
			try {
				if (read_data(true)) {
					if ((m_lock_item <= 0) || is_expired()) {
						m_lock_item = 1;
						m_process_id = processid;
						result = increment_data(time_out);
					} else if (m_process_id == processid) {
						m_lock_item++;
						result = increment_data(time_out);
					}
				}
			} catch (...) {}
			m_lock.unlock();
			return result;
		}
		bool decrement(const string& process_id) {
			bool result = false;
			m_lock.lock();
			try {
				if (read_data(false) && (m_process_id == process_id) && (m_lock_item > 0))
					result = decrement_data();
			} catch (...) {}
			m_lock.unlock();
			return result;
		}

		int try_lock(const string process_id, boost::posix_time::ptime &time_out) {
			int result = -1;
			if (!check())return result;
			return increment(process_id, time_out) ? 1:-1;
		}

	public:
		NamedBusiness_Lock_DatabaseItem(const string& name) :NamedBusiness_Lock_Item(name) {}
		int lock(const string process_id, int wait_for_lock_sec = NamedBusinessDefaultTimeOut(),
			int time_out_sec = NamedBusinessDefaultTimeOut()) {
			int result = -1;
			if (!check())return result;
			for (int t = 0, tout = wait_for_lock_sec * 1000; t < tout; t += 10) {
				if ((result = try_lock(process_id, time_out_sec)) > 0) break;
				::Sleep(10);
			}
			return result;
		}
		int unlock(const string process_id) {
			int result = -1;
			if (!check())return result;
			return decrement(process_id)? 1:-1;
		}
		int try_lock(const string process_id, int time_out_sec = NamedBusinessDefaultTimeOut()) {
			int result = -1;
			if (!check())return result;
			return increment(process_id, time_out_sec)? 1:-1;
		}
		int time_lock(const string process_id, boost::posix_time::ptime timeout,
			int wait_for_lock_sec = NamedBusinessDefaultTimeOut()) {
			int result = -1;
			if (!check())return result;
			for (int t = 0, tout = wait_for_lock_sec * 1000; t < tout; t += 10) {
				if ((result = try_lock(process_id, timeout)) > 0) break;
				::Sleep(10);
			}
			return result;
		}
		const string process_id() {
			string result = "";
			if (!check())return result;
			m_lock.lock();
			try {
				if (read_data(false) && (m_lock_item > 0) && !is_expired())
					result = m_process_id;
			}catch(...){}
			m_lock.unlock();
			return result;
		}
		void clear() {}
		int rescursive_lock_count() {
			int result = -1;
			if (!check())return result;
			result = 0;
			m_lock.lock();
			try {
				if (read_data(false) && (m_lock_item > 0) && !is_expired())
					result = m_lock_item;
			} catch (...) {}
			m_lock.unlock();
			return result;
		}
		static class std::shared_ptr<NamedBusiness_Lock_DatabaseItem> get_lock(const string  name) {
			return std::shared_ptr<NamedBusiness_Lock_DatabaseItem>(
				new NamedBusiness_Lock_DatabaseItem(name));
		}
	};


}

#endif