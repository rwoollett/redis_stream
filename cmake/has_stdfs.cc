//
// Created by rodney on 17/07/2020.
//
// == PROGRAM has_stdfs.cc ==

// Check if std::filesystem is available
// Source: https://stackoverflow.com/a/54290906
// with modifications

#include <filesystem>
namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    fs::path somepath{ "dir1/dir2/filename.txt" };
    auto fname = somepath.filename();
    return 0;
}