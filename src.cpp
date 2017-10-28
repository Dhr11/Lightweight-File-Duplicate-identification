#include <cstdlib>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <math.h>
#include <boost/filesystem.hpp>
#include <cstdlib>
namespace fs = boost::filesystem;
using namespace std;

// local includes
#include "head.hpp"

struct fileinfo {

    unsigned long int size;
    string path;

    bool operator < (const fileinfo &b) const
    {
        if      (size < b.size)  return true;
        else   return false;
    }

    // overload the < < operator for writing a BED struct
    friend ostream& operator<<(ostream &os, const fileinfo &b)
    {
        os  << b.size  << "\t"
            << b.path;
        return os;
    }
    friend istream& operator>>(istream &is, fileinfo &b)
    {
        is  >> b.size;
        getline( is,b.path,'\n');
        return is;
    }
    /*void operator=( const fileinfo &b)
    {
        size=b.size;
        path=b.path;
        cout<<" used"<<endl;
    }
*/
    // overload the >> operator for reading into a BED struct
};


// comparison function for sorting by chromosome, then by start.
bool bySize(fileinfo const &a, fileinfo const &b) {
    return (a.size) < (b.size);
}


int main(int argc, char* argv[]) {

    string inFile       = "info.fileinfo";
    int  bufferSize     = 4000;      // allow the sorter to use 100Kb (base 10) of memory for sorting.
                                       // once full, it will dump to a temp file and grab another chunk.
    string tempPath     = "./";        // allows you to write the intermediate files anywhere you want.

    string dir = argv[1];
    ofstream *filemapping;
    filemapping = new ofstream(inFile.c_str(), ios::out);
  	  for(fs::recursive_directory_iterator end, file(dir); file!=end; ++file) {
  		if(file->status().type()!=fs::regular_file) continue;
      fileinfo a;
      a.size = fs::file_size(*file);
      a.path= file->path().string();
      *filemapping<<a<<"\n";
  		    }
    filemapping->close();

    ostream *output;
    output = new ofstream("shortedLess.fileinfo", ios::out);

    KwayMergeSort<fileinfo> *filesize_sorter = new KwayMergeSort<fileinfo> (inFile,
                                                            output,
                                                            bySize,
                                                            bufferSize,
                                                            tempPath);

    cout << "First sort by size\n";
    filesize_sorter->Sort();

}
