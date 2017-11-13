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

    friend ostream& operator<<(ostream &os, const fileinfo &b)
    {
        os  << b.size  << "\t"
            << b.path;
        return os;
    }
    friend istream& operator>>(istream &is, fileinfo &b)
    {
        is  >> b.size;
        is.ignore(1024,'\t');
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
};


// comparison function for sorting by chromosome, then by start.
bool bySize(fileinfo const &a, fileinfo const &b) {
    return (a.size) < (b.size);
}


int main(int argc, char* argv[]) {

    string inFile       = "info.fileinfo";
    int  bufferSize     = 6000;        //limit each file to 6000 lines
    string tempPath     = "./";        // to write the temporary files at a location.

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

    FindDup<fileinfo> *filesize_sorter = new FindDup<fileinfo> (inFile,output,bySize,bufferSize,tempPath);

    cout << "First sort by size\n";
    filesize_sorter->Sort();

}
