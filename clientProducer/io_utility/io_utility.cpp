
#include "io_utility.h"
#include <iostream>
#include <set>
#include <fstream>
#ifdef _WINDOWS
#include <Windows.h>
#else
#include <locale.h>
#endif
#ifdef USE_EXP_FILESYSTEM
#include <experimental/filesystem>
#else
#include <filesystem>
#endif

namespace io_utility {
#ifdef USE_EXP_FILESYSTEM
	namespace fs = std::experimental::filesystem;
#else
	namespace fs = std::filesystem;
#endif

	void print(list slist, bool end_line) {
		for (auto s : slist) {
			std::cout << s;
		}
		if (end_line)
			std::cout << std::endl;
	}

	//The SQL standard specifies that single-quotes in strings are escaped 
	//by putting two single quotes in a row
	std::string sql_escape_single_quotes(std::string value) {
		std::string new_str{ "" };
		std::set<char> punctuation{ '\'' };
		for (char c : value) {
			auto it = punctuation.find(c);
			if (it != punctuation.end()) {
				// Put two single quotes
				new_str += c;
				new_str += c;
			}
			else {
				new_str += c;
			}
		}
		return new_str;
	}

	number_in get_number(std::string prompt) {
		int number;
		while (true) {
			std::cout << prompt;
			std::string user;
			if (getline(std::cin, user) && user.empty()) {
				return { 0, false };
			}
			try {
				number = std::stoi(user);
				auto chk = std::to_string(number);
				if (chk.length() != user.length()) {
					print({ "Got ", chk, " from ", user, " Are you sure, try again." });
					continue;
				}
				return { number, true };
			}
			catch (const std::exception /*&e*/) {
				/* do nothing */
			}
		}
	}

	char get_answer_choice(std::string prompt, std::set<char> choices) {
		while (true) {
			std::cout << prompt;
			std::string line;
			getline(std::cin, line);
			if (line.length() > 0) {
				auto it = choices.find(line[0]);
				if (it != choices.end()) {
					return *it;
				}
			}
		}
	}

	std::string create_file() {
		std::string filename;
		std::cout << "Save to what file: ";
		if (getline(std::cin, filename) && filename.empty()) {
			return "";
		}
		using namespace std;
		ofstream outfile;
		//outfile.exceptions(ios_base::badbit | ios_base::failbit); 
		if (outfile.open(filename), outfile.good()) {
			outfile.close();
			return filename;
		}
		else {
			print({ "Could not create \"", filename, "\"." });
		}
		return "";
	}

	std::vector<std::string> read_file(std::string file_name, bool strip_newline) {
		std::vector<std::string> lines;
		std::string line;
		using namespace std;
		ifstream infile;

		if (infile.open(file_name), infile.good()) {
			while (true) {
				if (infile.eof())
					break;
				std::getline(infile, line);
				if (line.empty())
					continue;
				if (strip_newline == false) {
					lines.push_back(std::string(line + '\n'));
				}
				else {
					lines.push_back(line);
				}
				if (infile.good())
					continue;
				else
					break;
			}
			if (infile.eof()) {
				infile.close();
			}
			else {
				print({ "Error reading file ", file_name });
			}
		}
		else {
			print({ "Error opening file ", file_name });
		}
		return lines;
	}

	void save_to_file(std::string file_name, std::vector<std::string> lines, bool add_newline,std::ios_base::openmode mode) {
		using namespace std;
		ofstream outfile;
		if (outfile.open(file_name, mode), outfile.good()) {
			for (auto line : lines) {
				outfile << line;
				if (add_newline == true)
					outfile << endl;
			}
			if (outfile.good()) {
				outfile.close();
			}
			else {
				print({ "Could not save file \"", file_name, "\"." });
			}
		}
	}

	void log_file(std::string file_name, std::vector<std::string> lines) {
		save_to_file(file_name, lines, false, std::ios::app);
	}
//
//	std::string io_utility::time_stamp() {
//		auto t = std::time(nullptr);
//		auto tm = *std::localtime(&t);
//		char mt[24];
//		if (0 < strftime(mt, sizeof(mt), "[%T %F] ", &tm)) {
//			return std::string(mt);
//		}
//		else {
//			return "[T F] ";
//		}
//	}


	void copy_file(std::string filename_in, std::string filename_out) {
		auto copy_lines = read_file(filename_in, false);
		// if truncate:
			// print("*** Truncating File Copy ***", len(copy_lines))
			// omit_start = get_number("Omit how many lines from the start:", positive_only=True)
			// omit_end = get_number("Omit how many lines from the end:", True)
			// if omit_start + omit_end >= len(copy_lines):
				// print("This will create empty file, no file \"{}\" is truncated copied.".format(filename_out))
				// return None
			// else:
				// copy_lines = truncate_list(copy_lines, omit_start, omit_end)
		save_to_file(filename_out, copy_lines, false);
	}

	std::string pick_file(std::string dir, std::string ext) {
		std::vector<std::string> file_list;  // files in the dir
		std::set<char> options{ 'q','Q' };
		std::set<char>::iterator o_iter;
		int pick;
		std::string user;
		for (auto & p : fs::directory_iterator(dir))
			if (is_regular_file(p) && p.path().extension() == ext)
				file_list.push_back(p.path().filename().string());

		while (1) {
			int fp = 1;
			for (auto n : file_list) {
				print({ std::to_string(fp), " ", n });
				fp++;
			}
			std::cout << "Pick file (or q): ";
			if (getline(std::cin, user) && options.find(user[0]) != options.end()) {
				return "";
			}
			try {
				pick = std::stoi(user);
				auto chk = std::to_string(pick);
				if (chk.length() != user.length()) {
					continue;
				}
				if (pick > 0 && pick <= (int)file_list.size())
					return file_list.at(pick - 1);
			}
			catch (const std::exception /*&e*/) {
				/* do nothing */
			}

		}
	}

	std::string to_uppercase(const char *name) {
		int counter = 0;
		char upd[40];
		char *p_upd = upd;
		while (name[counter]) {
			*p_upd = (char)toupper(name[counter]);
			p_upd++;
			counter++;
		}
		*p_upd = '\0';
		return std::string(upd);
	}


	std::wstring widen_string(const std::string &str) {
		//MB_ERR_INVALID_CHARS
#ifdef _WINDOWS
		int result = MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), NULL, 0);
		std::wstring text_wchar(result, 0);
		MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), &text_wchar[0], result);
#else // _OTHER
		setlocale(LC_ALL, "en_NZ");
		//locale_t lc = get_current_locale();
		size_t length = 0;
//		std::cout << "widen\n";
		//std::string temp(str);
        std::wstring text_wchar(10, L'0');
		mbstowcs(&text_wchar[0], &str[0], 10);
//		std::cout << "length:" << length << "\n";
//		if (length > 0)
//			_mbstowcs_s_l(NULL, &text_wchar[0], length + 1, &temp[0], length, setlocale(LC_ALL, NULL));
#endif // _WINDOWS

		return text_wchar;
	}

	std::string narrow_string(const std::wstring &str) {

#ifdef _WINDOWS
		int result = WideCharToMultiByte(CP_UTF8, 0, &str[0], (int)str.size(), NULL, 0, NULL, NULL);
		//1004
		//DWORD err = GetLastError();
		std::string text_char(result, 0);
		WideCharToMultiByte(CP_UTF8, 0, &str[0], (int)str.size(), &text_char[0], result, NULL, NULL);
#else // _OTHER
		setlocale(LC_ALL, "en_NZ");
		//_locale_t lc = _get_current_locale();
		//size_t length = 0;
		//_wcstombs_s_l(&length, NULL, 0, &str[0], 0, setlocale(LC_ALL, NULL));
		std::string text_char(10, '0');
		//if (length > 0)
		wcstombs(&text_char[0], &str[0], 10);
#endif // _WINDOWS

		return text_char;
	}

	std::vector<std::string> split_string(const std::string &in, const char &sep) {
		std::string::size_type b = 0;
		std::vector<std::string> result;
		while ((b = in.find_first_not_of(sep, b)) != std::string::npos) {
			auto e = in.find_first_of(sep, b);
			auto item = in.substr(b, e - b);
			item.erase(item.find_last_not_of(" \n\r\t") + 1);
			item.erase(0, item.find_first_not_of(" \n\r\t"));
			result.push_back(item);
			b = e;
		}
		return result;
	}
};
