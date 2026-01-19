
#ifndef _IO_UTILITY_H_
#define _IO_UTILITY_H_

#include <string>
#include <tuple>
#include <vector>
#include <set>
#include <fstream>
typedef std::tuple<int, bool> number_in;
typedef std::vector<std::string> list;
typedef unsigned short io_word;
typedef unsigned char io_byte;

namespace io_utility {
	
	void print(list slist, bool end_line=true);
	std::string sql_escape_single_quotes(std::string value);
	number_in get_number(std::string prompt);
	char get_answer_choice(std::string prompt, std::set<char> choices);
	std::string create_file();
    void save_to_file(std::string file_name, std::vector<std::string> lines, bool add_newline=false, std::ios_base::openmode mode=std::ios::out);
	void log_file(std::string file_name, std::vector<std::string> lines);
	std::vector<std::string> read_file(std::string file_name, bool strip_newline=false);
    void copy_file(std::string filename_in, std::string filename_out);
    std::string to_uppercase(const char *name);
	std::wstring widen_string(const std::string &str);
	std::string narrow_string(const std::wstring &str);

	std::vector<std::string> split_string(const std::string &in, const char &sep);

#ifdef _WINDOWS
	std::string pick_file(std::string dir, std::string ext=".txt");
#endif

};


#endif //_IO_UTILITY_H_
