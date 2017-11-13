#ifndef HEAD_H
#define HEAD_H
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
      // priority queues sort from highest to lowest. so negate.
      if(a.data.size==data.size)
      {
        return (hash.compare(a.hash));
      }
      return false;
  }
};

template <class T>
void internalhash_calculator(string filename,int size)
{
  cout<<"enter thread size:"<<size<<" filename: "<<filename<<endl;
  ifstream *file;
  stringstream ss2;
  ss2<<"hash"<<filename;
  ofstream *output2;
  output2 = new ofstream(ss2.str(), ios::out);// making new file and empty previous copy
  output2->close();
  delete output2;

  if (isRegularFile(filename) == true) {
    file = new ifstream(filename.c_str(), ios::in);
    vector<internalhash<T>> lineBuffer;
    T line;
    unsigned long int cursize(0);
    while(*file>>line)
    {
      internalhash<T> item;
      int file_descript;
      if(line.size==0)
      {
        item.hash="0";
        item.data=line;
        lineBuffer.push_back(item);
        continue;
      }
      if(line.size!=cursize)
      {
        if(!lineBuffer.empty())
        {
          sort(lineBuffer.begin(), lineBuffer.end());
          stringstream ss;
          ss<<"hash"<<filename;
          ofstream *output;
          output = new ofstream(ss.str(), ios::out|ios_base::app|ios::ate);
          // write the contents of the current buffer to the temp file
          for (size_t i = 0; i < lineBuffer.size(); ++i) {
              *output << lineBuffer[i] << endl;
          }
          lineBuffer.clear();
          output->close();
          delete output;
          cursize=line.size;
                  }
      }
      file_descript = open(line.path.c_str(), O_RDONLY,(mode_t)0600);

      if(file_descript < 0)
      {
        item.hash="0";
        item.data=line;
        lineBuffer.push_back(item);
        close(file_descript);
        continue;
      }
      unsigned long file_size;
      if(size==0)
      file_size=line.size;
      else
      file_size=size;
      std::ostringstream sout2;



      char* file_buffer = static_cast<char*>(mmap(0, file_size, PROT_READ, MAP_PRIVATE, file_descript, 0));
      MD5((unsigned char*) file_buffer, file_size, result);
      munmap(file_buffer, file_size);
      sout2<<std::hex<<std::setfill('0');
      for(auto c: result) sout2<<setw(2)<<(int)c;


      item.hash=sout2.str();
      item.data=line;
      lineBuffer.push_back(item);
      close(file_descript);
    }
    stringstream ss;
    ss<<"hash"<<filename;
    ofstream *output;
    output = new ofstream(ss.str(), ios::out|ios_base::app|ios::ate);
    // write the contents of the current buffer to the temp file
    for (size_t i = 0; i < lineBuffer.size(); ++i) {
        *output << lineBuffer[i] << endl;
    }
    lineBuffer.clear();
    output->close();
    delete output;

  }

}
template <class T>
class MERGE_DATA {

public:
    T data;
    istream *stream;
    bool (*compFunc)(const T &a, const T &b);
    MERGE_DATA ()
    {
      data.size=0;data.path="";
      stream=NULL;
      compFunc=NULL;
    }

    MERGE_DATA (const T &data,
                istream *stream,
                bool (*compFunc)(const T &a, const T &b))
    :   data(data),stream(stream),compFunc(compFunc)
    {}

    bool operator < (const MERGE_DATA &a) const
    {
    // priority queues sort highest to lowest. so negate.
        return !(compFunc(data, a.data));
    }
};


template <class T>
class FindDup {

public:

    // constructor, using custom comparison function
    FindDup(const string &inFile,
                 ostream *out,
                 bool (*compareFunction)(const T &a, const T &b) = NULL,
                 int  maxBufferSize  = 1000000,
                 string tempPath     = "./");

    // destructor
    ~FindDup(void);

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
    std::thread t[5];
    vector<int> indexes;
    void DivideAndSort();
    void Merge_Partition();
    void WTempfile(const vector<T> &lines, const string name);
    void OTempFiles();
    void CTemppFiles();
    void HashReduce();
};


template <class T>
FindDup<T>::FindDup (const string &inFile,
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
  _vHashFileNames[5000]="smallest";//5kb
  _vHashFileNames[500000]="small";//500kb
  _vHashFileNames[30000000]="moderate";//30mb
  _vHashFileNames[700000000]="huge";//700mb
  _vHashFileNames[100000000000]="humungous";//100gb
  //_vHashFileNames[]=;
}

template <class T>
FindDup<T>::~FindDup(void)
{}

// API for sorting.
template <class T>
void FindDup<T>::Sort() {
    DivideAndSort();
    cin.get();
    if (_tempFileUsed == true)
    {
      Merge_Partition();
      cout<<"after Merge_Partition()"<<endl;
      HashReduce();
    }
}

// change the buffer size used for sorting
template <class T>
void FindDup<T>::SetBufferSize (int bufferSize) {
    _maxBufferSize = bufferSize;
}

// change the sorting criteria
template <class T>
void FindDup<T>::SetComparison (bool (*compareFunction)(const T &a, const T &b)) {
    _compareFunction = compareFunction;
}


template <class T>
void FindDup<T>::DivideAndSort() {

    istream *input = new ifstream(_inFile.c_str(), ios::in);
    if ( input->good() == false ) {
        cerr << "Error: file (" << _inFile << ") could not be opened. Exit" << endl;
        exit (1);
    }
    vector<T> lineBuffer;
    lineBuffer.reserve(_maxBufferSize);
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.

    _tempFileUsed = false;

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
            WTempfile(lineBuffer,"null");
            lineBuffer.clear();
            _tempFileUsed = true;
            totalBytes = 0;
        }
    }
cout<<"total lines: "<<_count<<endl;
    // handle the run (if any) from the last chunk of the input file.
    if (lineBuffer.empty() == false) {
        // write the last "chunk" to file if file had to be used
        if (_tempFileUsed == true) {
            if (_compareFunction != NULL)
                {
                  cout<<"lastchunk: "<<*lineBuffer.begin()<<endl;
                  sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
                  cout<<"last chuunk: "<<*lineBuffer.begin()<<endl;
                }
            else
                sort(lineBuffer.begin(), lineBuffer.end());

            WTempfile(lineBuffer,"null");
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
          }
        }
    }
}


template <class T>
void FindDup<T>::WTempfile(const vector<T> &lineBuffer,const string name) {
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
      cout<<name<<endl;
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

template <class T>
void FindDup<T>::Merge_Partition() {

    OTempFiles();

    // priority queue for the buffer.
    priority_queue< MERGE_DATA<T> > outQueue;
    // extract the first line from each temp file
    T line;
    for (size_t i = 0; i < _vTempFiles.size(); ++i) {
        *_vTempFiles[i] >> line;
        outQueue.push( MERGE_DATA<T>(line, _vTempFiles[i], _compareFunction) );
    }

    std::map<unsigned long int, string>::iterator itr2 = _vHashFileNames.begin();
    while (itr2 != _vHashFileNames.end())
    {
    //cout<<" itr2 sec"<<itr2->second.c_str()<<endl;
    ofstream *output2;
    output2 = new ofstream(itr2->second.c_str(), ios::out);// making new file and empty previous copy
    output2->close();
    itr2++;
  }

  map<string,unsigned long int> sizes;
  sizes["smallest"]=0;// full or no hash
  sizes["small"]=5000;
  sizes["moderate"]=500000;//800kb
  sizes["huge"]=10000000;//15mb
  sizes["humungous"]=50000000;//100mb

  map<string,int> sizestothread;
  sizestothread["smallest"]=0;// full or no hash
  sizestothread["small"]=1;
  sizestothread["moderate"]=2;//800kb
  sizestothread["huge"]=3;//15mb
  sizestothread["humungous"]=4;


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
            WTempfile(lineBuffer, itr->second);
            lineBuffer.clear();
            //cout<<"itr sec: "<<itr->second<<" sizes[itr->second]"<<sizes[itr->second]<<" sizestothread[itr->second]:"<<sizestothread[itr->second]<<endl;
            t[sizestothread[itr->second]]=std::thread(internalhash_calculator<T>,itr->second,sizes[itr->second]);
            indexes.push_back(sizestothread[itr->second]);
            }
        }

      else
      {
        outQueue.push(past);
        if(!lineBuffer.empty())
        {
          WTempfile(lineBuffer, itr->second);
          lineBuffer.clear();
          //cout<<"itr sec: "<<itr->second<<" sizes[itr->second]"<<sizes[itr->second]<<" sizestothread[itr->second]:"<<sizestothread[itr->second]<<endl;
          t[sizestothread[itr->second]]=std::thread(internalhash_calculator<T>,itr->second,sizes[itr->second]);
          indexes.push_back(sizestothread[itr->second]);
        }
        itr++;
        continue;
      }

    }

  }
    // clean up the temp files.
    CTemppFiles();
}


template <class T>
void FindDup<T>::OTempFiles() {
    for (size_t i=0; i < _vTempFileNames.size(); ++i) {

        int yes(0);
        if (isRegularFile(_vTempFileNames[i]) == true) {
        ifstream *file = new ifstream(_vTempFileNames[i].c_str(), ios::in);
            if (file->good() == true) {
                // add a pointer to the opened temp file to the list
                _vTempFiles.push_back(file);
                yes=1;
            }
        }

        if(yes==0)
        {    cerr << "Unable to open temp file (" << _vTempFileNames[i]<< ").Exiting."<< endl;
             exit(1);
        }
    }
}

template <class T>
void FindDup<T>::HashReduce () {

    for(auto const& value: indexes) {
      cout<<"join thread:"<<value<<endl;
      t[value].join();
    }
}


template <class T>
void FindDup<T>::CTemppFiles() {
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
   can be opened/closed/seek'd multiple times without losing information
 */
bool isRegularFile(const string& filename) {
       struct stat buf ;
       int i;

       i = stat(filename.c_str(), &buf);
       if (i!=0) {
               cerr << "Error: file'" << filename << "': " << strerror(errno);
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


#endif
