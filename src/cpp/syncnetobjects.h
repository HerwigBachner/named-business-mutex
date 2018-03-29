#ifndef HB_TOOLS_SYNC_NET_OBJECTS
#define HB_TOOLS_SYNC_NET_OBJECTS
#include <string>
#include <vector>
#include <boost/asio.hpp>

namespace hb{
	using namespace std;
	using boost::asio::ip::tcp;




	class SyncNetCommunication {
	private:
		boost::asio::io_service m_io_service;
		vector<string> m_endpoints;
		tcp::endpoint m_server_endpoint;
		tcp::acceptor m_acceptor;
		tcp::socket m_socket;

	private:
		void do_accept() {
			m_acceptor.async_accept(m_socket,
				[this](boost::system::error_code ec) {
				if (!ec) {
					make_shared<chat_session>(move(m_socket), room_)->start();
				}

				do_accept();
			});
		}

	public:
		SyncNetCommunication(const vector<string> endpoints,int serverport):m_endpoints(endpoints),
			m_acceptor(m_io_service,tcp::endpoint(tcp::v4(), serverport)),m_socket(m_io_service){
			do_accept();
		}

	};


}


#endif


