#ifndef KWAYMERGESORT_H
#define KWAYMERGESORT_H
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include <vector>
#include <queue>
#include <cstdio>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <map>
#include <openssl/md5.h>
#include<thread>
#include<iomanip>
#include <fcntl.h>
#include <libgen.h> //for basename()
using namespace std;

unsigned char result[MD5_DIGEST_LENGTH];
bool isRegularFile(const string& filename);
// STLized version of basename()
// (because POSIX basename() modifies the input string pointer)
// Additionally: removes any extension the basename might have.
std::string stl_basename(const std::string& path);


template <class T>
class internalhash
{
public:
  string hash;
  T data;

  friend ostream& operator<<(ostream &os, const internalhash &b)
  {
      os  << b.hash  << "\t"
          << b.data;
      return os;
  }
  friend istream& operator>>(istream &is, internalhash &b)
  {
      is  >> b.hash;
      getline( is,b.data,'\n');
      return is;
  }
  bool operator < (const internalhash &a) const
  {
      // recall that priority queues try to sort from
      // highest to lowest. thus, we need to negate.
      if(a.data.size==data.size)
      {
        return (data.path.compare(a.data.path));
      }
      return false;
  }
};

template <class T>
void internalhash_calculator(string filename,int size)
{
  cout<<"enter thread size:"<<size<<" filename: "<<filename<<endl;
  ifstream *file;

  if (isRegularFile(filename) == true) {
    file = new ifstream(filename.c_str(), ios::in);
    vector<internalhash<T>> lineBuffer;
    T line;

    while(*file>>line)
    {
      cout<<line.size<<"\t";
      internalhash<T> item;
      int file_descript;
      if(line.size==0)
      {
        cout<<"?";
        item.hash="0";
        item.data=line;
        lineBuffer.push_back(item);
        continue;
      }
      //stringstream ss;
      //ss<<'"'<<line.path.c_str()<<'"';
      //cout<<ss.str().c_str()<<" "<<endl;
      file_descript = open(line.path.c_str(), O_RDONLY,(mode_t)0600);

      if(file_descript < 0)
      {
        cout<<"!";
        //stringstream item;
        //item<<"0"<<line;
        //internalhash<T> item;
        item.hash="0";
        item.data=line;
        lineBuffer.push_back(item);
        close(file_descript);
        continue;
      }
      cout<<"#";
      unsigned long file_size;
      if(size==0)
      file_size=line.size;
      else
      file_size=size;
      std::ostringstream sout2;



      char* file_buffer = static_cast<char*>(mmap(0, file_size, PROT_READ, MAP_SHARED, file_descript, 0));
      MD5((unsigned char*) file_buffer, file_size, result);
      munmap(file_buffer, file_size);
      sout2<<std::hex<<std::setfill('0');
      for(auto c: result) sout2<<setw(2)<<(int)c;


      cout<<sout2.str()<<endl;

      item.hash=sout2.str();
      item.data=line;
      lineBuffer.push_back(item);
      close(file_descript);
    }
    stringstream ss;
    ss<<"hash"<<filename;
    ofstream *output;
    output = new ofstream(ss.str(), ios::out);
    // write the contents of the current buffer to the temp file
    for (size_t i = 0; i < lineBuffer.size(); ++i) {
        *output << lineBuffer[i] << endl;
    }
    output->close();
    delete output;

  }

}
template <class T>
class MERGE_DATA {

public:
    // data
    T data;
    istream *stream;
    bool (*compFunc)(const T &a, const T &b);

    // constructor

    MERGE_DATA ()
    {
      data.size=0;data.path="";
      stream=NULL;
      compFunc=NULL;
    }

    MERGE_DATA (const T &data,
                istream *stream,
                bool (*compFunc)(const T &a, const T &b))
    :
        data(data),
        stream(stream),
        compFunc(compFunc)
    {}

    // comparison operator for maps keyed on this structure
    bool operator < (const MERGE_DATA &a) const
    {
        // recall that priority queues try to sort from
        // highest to lowest. thus, we need to negate.
        return !(compFunc(data, a.data));
    }
};


//************************************************
// DECLARATION
// Class methods and elements
//************************************************
template <class T>
class KwayMergeSort {

public:

    // constructor, using custom comparison function
    KwayMergeSort(const string &inFile,
                 ostream *out,
                 bool (*compareFunction)(const T &a, const T &b) = NULL,
                 int  maxBufferSize  = 1000000,
                 string tempPath     = "./");

    // destructor
    ~KwayMergeSort(void);

    void Sort();            // Sort the data
    void SetBufferSize(int bufferSize);   // change the buffer size
    void SetComparison(bool (*compareFunction)(const T &a, const T &b));   // change the sort criteria

private:
    string _inFile;
    bool (*_compareFunction)(const T &a, const T &b);
    string _tempPath;
    vector<string>    _vTempFileNames;
    map<unsigned long int,string>    _vHashFileNames;
    vector<ifstream*>  _vTempFiles;
    unsigned int _maxBufferSize;
    unsigned int _runCounter;
    bool _tempFileUsed;
    ostream *_out;
    int _count;  //no of lines
    // drives the creation of sorted sub-files stored on disk.
    void DivideAndSort();
    void Merge();
    void WriteToTempFile(const vector<T> &lines, const string name);
    void OpenTempFiles();
    void CloseTempFiles();
    void HashReduce();
};



//************************************************
// IMPLEMENTATION
// Class methods and elements
//************************************************

// constructor
template <class T>
KwayMergeSort<T>::KwayMergeSort (const string &inFile,
                               ostream *out,
                               bool (*compareFunction)(const T &a, const T &b),
                               int maxBufferSize,
                               string tempPath)
    : _inFile(inFile)
    , _out(out)
    , _compareFunction(compareFunction)
    , _tempPath(tempPath)
    , _maxBufferSize(maxBufferSize)
    , _runCounter(0)
    , _count(0)
{
  _vHashFileNames[10000]="smallest";//10kb
  _vHashFileNames[1000000]="small";//1mb
  _vHashFileNames[40000000]="moderate";//40mb
  _vHashFileNames[700000000]="huge";//700mb
  _vHashFileNames[100000000000]="humungous";//100gb
  //_vHashFileNames[]=;
}

// constructor
// destructor
template <class T>
KwayMergeSort<T>::~KwayMergeSort(void)
{}

// API for sorting.
template <class T>
void KwayMergeSort<T>::Sort() {
    DivideAndSort();
    cin.get();
    if (_tempFileUsed == true)
    {
      Merge();
      HashReduce();
    }
}

// change the buffer size used for sorting
template <class T>
void KwayMergeSort<T>::SetBufferSize (int bufferSize) {
    _maxBufferSize = bufferSize;
}

// change the sorting criteria
template <class T>
void KwayMergeSort<T>::SetComparison (bool (*compareFunction)(const T &a, const T &b)) {
    _compareFunction = compareFunction;
}


template <class T>
void KwayMergeSort<T>::DivideAndSort() {

    istream *input = new ifstream(_inFile.c_str(), ios::in);
    if ( input->good() == false ) {
        cerr << "Error: The requested input file (" << _inFile << ") could not be opened. Exiting!" << endl;
        exit (1);
    }
    vector<T> lineBuffer;
    lineBuffer.reserve(_maxBufferSize);
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.

    // track whether or not we actually had to use a temp
    _tempFileUsed = false;

    // keep reading until there is no more input data
    T line;
    _count=0;
    while (*input >> line) {
        lineBuffer.push_back(line);
        totalBytes += sizeof(line);  // check later
        _count++;
        // sort the buffer and write to a temp file if we have filled up our quota
        if (_count %_maxBufferSize==0) {
            if (_compareFunction != NULL)
                {
                  sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
                }
            else
                sort(lineBuffer.begin(), lineBuffer.end());
            // write the sorted data to a temp file
            WriteToTempFile(lineBuffer,"null");
            // clear the buffer for the next run
            lineBuffer.clear();
            _tempFileUsed = true;
            totalBytes = 0;
        }
    }
cout<<"total lines: "<<_count<<endl;
    // handle the run (if any) from the last chunk of the input file.
    if (lineBuffer.empty() == false) {
        // write the last "chunk" to the tempfile if
        // a temp file had to be used (i.e., we exceeded the memory)
        if (_tempFileUsed == true) {
            if (_compareFunction != NULL)
                {
                  cout<<"lastchunk: "<<*lineBuffer.begin()<<endl;
                  sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
                  cout<<"last chuunk: "<<*lineBuffer.begin()<<endl;
                }
            else
                sort(lineBuffer.begin(), lineBuffer.end());
            // write the sorted data to a temp file
            //WriteToTempFile(lineBuffer);
            WriteToTempFile(lineBuffer,"null");
        }
        // otherwise, the entire file fit in the memory given,
        // so we can just dump to the output.
        else {
            if (_compareFunction != NULL)
                {
                  cout<<"file fit mem: "<<*lineBuffer.begin()<<lineBuffer.size()<<endl;
                  sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
                  cout<<"after sort file fit mem: "<<*lineBuffer.begin()<<lineBuffer.size()<<endl;
                }
            else
                sort(lineBuffer.begin(), lineBuffer.end());
            unsigned long int curmultiple(0);
            T past=lineBuffer[0];
            for (size_t i = 1; i < lineBuffer.size(); ++i)
          {
            if((lineBuffer[i].size==past.size) || (past.size==curmultiple))
            {
              curmultiple=past.size;
              *_out << past << endl;
              past=lineBuffer[i];
            }
              *_out << past << endl;
            //*_out << lineBuffer[i] << endl;
          }
        }
    }
}


template <class T>
void KwayMergeSort<T>::WriteToTempFile(const vector<T> &lineBuffer,const string name) {
    stringstream tempFileSS;
    string tempFileName;
    if((name.compare("null")==0))
    {
      if (_tempPath.size() == 0)
        tempFileSS << _inFile << "." << _runCounter;
    else
        tempFileSS << _tempPath << "/" << stl_basename(_inFile) << "." << _runCounter;
    tempFileName = tempFileSS.str();
    ++_runCounter;
    _vTempFileNames.push_back(tempFileName);
    }
    else
    {
      tempFileName = name;
    }
    ofstream *output;
    output = new ofstream(tempFileName.c_str(), ios::out);
    // write the contents of the current buffer to the temp file
    for (size_t i = 0; i < lineBuffer.size(); ++i) {
        *output << lineBuffer[i] << endl;
    }

    // update the tempFile number and add the tempFile to the list of tempFiles
    output->close();
    delete output;


}


//---------------------------------------------------------
// MergeDriver()
//
// Merge the sorted temp files.
// uses a priority queue, with the values being a pair of
// the record from the file, and the stream from which the record came.
// SEE: http://stackoverflow.com/questions/2290518/c-n-way-merge-for-external-sort, post from Eric Lippert.
//----------------------------------------------------------
template <class T>
void KwayMergeSort<T>::Merge() {

    // open the sorted temp files up for merging.
    // loads ifstream pointers into _vTempFiles
    OpenTempFiles();

    // priority queue for the buffer.
    priority_queue< MERGE_DATA<T> > outQueue;

    // extract the first line from each temp file
    T line;
    for (size_t i = 0; i < _vTempFiles.size(); ++i) {
        *_vTempFiles[i] >> line;
        outQueue.push( MERGE_DATA<T>(line, _vTempFiles[i], _compareFunction) );
    }
    cout<<"outQueue size:"<<outQueue.size()<<endl;
    // keep working until the queue is empty
    //MERGE_DATA<T> past;

    if(outQueue.size()>1)
    {
      std::map<unsigned long int, string>::iterator itr = _vHashFileNames.begin();
    unsigned long int curmultiple(0);
    vector<T> lineBuffer;
    while (itr != _vHashFileNames.end())
    {
      if(outQueue.empty())
      {
        itr++;
        continue;
      }
      MERGE_DATA<T> past = outQueue.top();

      outQueue.pop();
      if(past.data.size<itr->first)
      {
        lineBuffer.push_back(past.data);
        if (*(past.stream))
          outQueue.push( MERGE_DATA<T>(line, past.stream, _compareFunction) );
        //cout<<"past or first info: "<<past.data<<endl;
        while (outQueue.empty() == false) {
            bool insert=false;
            MERGE_DATA<T> lowest = outQueue.top();
            if(lowest.data.size>itr->first)
            {
              break;
            }
            if((lowest.data.size==past.data.size) || (past.data.size==curmultiple))
            {
              curmultiple=past.data.size;
              lineBuffer.push_back(past.data);//*_out << past.data << endl;
              insert=true;
            }
            past=lowest;
            outQueue.pop();// remove this record from the queue
            *(lowest.stream) >> line;// add the next line from the lowest stream (above) to the queue
            if (*(lowest.stream))
                outQueue.push( MERGE_DATA<T>(line, lowest.stream, _compareFunction) );

        }
        if((outQueue.empty()) && past.data.size==curmultiple)  ///check if insert is required
          { cout<<"last one:)"<<endl;
            lineBuffer.push_back(past.data);
            WriteToTempFile(lineBuffer, itr->second);
            lineBuffer.clear();
            }
        }

      else
      {
        outQueue.push(past);
        if(!lineBuffer.empty())
        {
          WriteToTempFile(lineBuffer, itr->second);
          lineBuffer.clear();
        }
        itr++;
        continue;
      }

    }

  }
    // clean up the temp files.
    CloseTempFiles();
}


template <class T>
void KwayMergeSort<T>::OpenTempFiles() {
    for (size_t i=0; i < _vTempFileNames.size(); ++i) {

        ifstream *file;

        if (isRegularFile(_vTempFileNames[i]) == true) {
            file = new ifstream(_vTempFileNames[i].c_str(), ios::in);
        }
        if (file->good() == true) {
            // add a pointer to the opened temp file to the list
            _vTempFiles.push_back(file);
        }
        else {
            cerr << "Unable to open temp file (" << _vTempFileNames[i]
                 << ").  I suspect a limit on number of open file handles.  Exiting."
                 << endl;
             exit(1);
        }
    }
}

template <class T>
void KwayMergeSort<T>::HashReduce () {
    std::map<unsigned long int, string>::iterator itr = _vHashFileNames.begin();
    map<int,unsigned long int> sizes;
    sizes[0]=0;// full or no hash
    sizes[1]=10000;
    sizes[2]=1000000;//1mb
    sizes[3]=15000000;//15mb
    sizes[4]=100000000;//100mb
    std::thread t[_vHashFileNames.size()];
    int i=0;
    while(itr!=_vHashFileNames.end())
    {
      cout<<"i: "<<i<<endl;
      t[i]=std::thread(internalhash_calculator<T>,itr->second,sizes[i]);
      i++;
      itr++;
    }
    for (i = 0; i < _vHashFileNames.size(); ++i) {
    t[i].join();
    }
}


template <class T>
void KwayMergeSort<T>::CloseTempFiles() {
    // delete the pointers to the temp files.
    for (size_t i=0; i < _vTempFiles.size(); ++i) {
        _vTempFiles[i]->close();
        delete _vTempFiles[i];
    }
    // delete the temp files from the file system.
    for (size_t i=0; i < _vTempFileNames.size(); ++i) {
        remove(_vTempFileNames[i].c_str());  // remove = UNIX "rm"
    }
    _runCounter=0;
}


/*
   returns TRUE if the file is a regular file:
     not a pipe/device.
   This implies that the file can be opened/closed/seek'd multiple times without losing information
 */
bool isRegularFile(const string& filename) {
       struct stat buf ;
       int i;

       i = stat(filename.c_str(), &buf);
       if (i!=0) {
               cerr << "Error: can't determine file type of '" << filename << "': " << strerror(errno) << endl;
               exit(1);
       }
       if (S_ISREG(buf.st_mode))
               return true;

       return false;
}

string stl_basename(const string &path) {
    string result;

    char* path_dup = strdup(path.c_str());
    char* basename_part = basename(path_dup);
    result = basename_part;
    free(path_dup);

    size_t pos = result.find_last_of('.');
    if (pos != string::npos )
        result = result.substr(0,pos);

    return result;
}


#endif /* KWAYMERGESORT_H */
