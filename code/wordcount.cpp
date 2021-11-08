// wordcount.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <stdio.h>
#include "mpi.h"
#include <malloc.h>
#include <string.h>
#include <sys/stat.h>
//#include <io.h> 
#include <sys/types.h>
#include <ctype.h>
#include <map>
#include <vector>
#include <string>
#include <dirent.h>

#define WORD_MAX_LEN 1024
#define SEGMENT_LEN (1024*1024)
#define FREQ_LEN (256*1024)

#define TAGFORWORKERSATTE 0
#define TAGFORTASK 1
#define TAGFORRESULT 2

using namespace std;

const char separators[] = " *!@$%^&_|{};?/`~,=-.[]:#()'!?\"\t\n";

typedef struct wordcnt {
    char word[WORD_MAX_LEN];
    int freq;
}WCP;



void readfiledirectory(vector<string> &files, string basepath)
{
    DIR* dir = NULL;
    struct dirent* entry;
    if ((dir = opendir(basepath.c_str())) == NULL)
    {
        printf("opendir failed!");
        return;
    }
    else
    {
        while ((entry = readdir(dir)) != NULL)
        {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            else if (entry->d_type == DT_REG)
            {
                string filepath = basepath + entry->d_name;
                files.push_back(filepath);
            }
                
        }
        closedir(dir);
    }
}


bool IsAlphaOrDigit(char c)
{
    return (c > 64 && c < 91) || (c > 96 && c < 123)||(c> 47&& c<58);
}

void readfile(char* filename, char*& wordresult, int& mlength, int*& freqresult, int& freqlen)
{
    int filelen;
    FILE* fp = fopen(filename, "r");
    fseek(fp, 0, SEEK_END);
    filelen = ftell(fp);
    if (filelen == 0)
    {
        printf("empty file '%s'\n", filename);
        fclose(fp);
        return;
    }
    else
    {
        char* wordbuff = (char*)malloc(filelen*sizeof(char));
        fseek(fp, 0, SEEK_SET);
        printf("reading '%s' of length %d\n", filename, filelen);
        fread(wordbuff, filelen, 1, fp);
        fclose(fp);
        map<string, int> wordcntcp;
        map<string, int>::iterator it;
        char word[WORD_MAX_LEN];
        memset(word, 0, sizeof(word));
        int j = 0;
        for (int i = 0; i < filelen; i++)
        {
            if (IsAlphaOrDigit(wordbuff[i]))
            {
                word[j++] = tolower(wordbuff[i]);
            }
            else if ((i+1<filelen)&&(i-1>0)&&wordbuff[i] == '-' && IsAlphaOrDigit(wordbuff[i - 1]) && IsAlphaOrDigit(wordbuff[i + 1]))
            {
                word[j++] = tolower(wordbuff[i]);
            }
            else if ((i + 1 < filelen) && (i - 1 > 0)&&wordbuff[i] == '.'&&isdigit(wordbuff[i - 1])&& isdigit(wordbuff[i + 1]))
            {
                word[j++] = tolower(wordbuff[i]);
            }
            else
            {
                word[j] = '\0';
                it = wordcntcp.find(word);
                if (it == wordcntcp.end())
                {
                    string wordtmp = word;
                    wordcntcp.insert(make_pair(wordtmp, 1));
                }
                else
                {
                    it->second++;
                }
                for (; i < filelen && !IsAlphaOrDigit(wordbuff[i]); i++);
                i--;
                j = 0;
                memset(word, 0, sizeof(word));
            }
        }
        int mtlength = 0;
        char* mtxt = (char*)calloc(filelen, sizeof(char));
        int* freq = (int*)calloc(wordcntcp.size(), sizeof(int));
        int freqtlen = 0;
        for (it = wordcntcp.begin(); it != wordcntcp.end(); it++)
        {
            strcat(mtxt, it->first.c_str());
            mtlength += it->first.length();
            strcat(mtxt, ",");
            mtlength++;
            freq[freqtlen++] = it->second;
        }
//        mtxt[mlength++] = '\0';
        printf("wordresult:%s\n", mtxt);
        printf("mtlength: %d\n", mtlength);
        printf("strlen(mxtxt): %d\n", strlen(mtxt));
        printf("freqtlen: %d\n", freqtlen);
        printf("wordcntcp.size: %d\n", wordcntcp.size());
        wordresult = mtxt;
        mlength = mtlength;
        freqresult = freq;
        freqlen = freqtlen;
    }
}


void resultrestore(map<string, int>& mapresult, char* wordresult, int mlength, int* freqresult, int freqlen)
{
    char word[WORD_MAX_LEN];
    int wordi = 0;
    map<string, int>::iterator it;
    int wordnum = 0;
    int wordfreq = 0;
    for (int i = 0; i < mlength && wordnum < freqlen; i++)
    {
        if (wordresult[i] != ',')
        {
            word[wordi++] = wordresult[i];
        }
        else
        {
            word[wordi] = '\0';
            printf("%s   ", word);
            wordi = 0;
            it = mapresult.find(word);
            wordfreq = freqresult[wordnum];
            if (it == mapresult.end())
            {
                string wordtmp = word;
                mapresult.insert(make_pair(wordtmp, wordfreq));
            }
            else
            {
                it->second += wordfreq;
            }
            memset(word, 0, sizeof(word));
            wordnum++;
        }
    }
    printf("manager wordnum : %d \n", wordnum);
}

void manager(int p);
void worker(int id);
int read_file_name();
int main(int argc, char* argv[])
{
    int id;
    int p;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    MPI_Comm_size(MPI_COMM_WORLD, &p);
    double my_elapsed_time = 0.0;
    my_elapsed_time = -MPI_Wtime();
    if (!id)
    {
//        MPI_Barrier(MPI_COMM_WORLD);
        manager(p);
//        MPI_Barrier(MPI_COMM_WORLD);
    }
    else {
//        MPI_Barrier(MPI_COMM_WORLD);
        worker(id);
//         MPI_Barrier(MPI_COMM_WORLD);
    }
    my_elapsed_time += MPI_Wtime();
    if (!id)
    {
        printf("my_elapsed_time:%f\n", my_elapsed_time);
    }
    MPI_Finalize();
    return 0;
}
void manager(int p)
{
    int* power;
    MPI_Status status;
    char* filename;
    const char* FilePath = "./project_file/small_file/";
    //    char c;
    int i;
    //   int file_size;
    int terminate;
    terminate = 0;
    //   file_size = read_file_name();   // 读取文件个数	
    filename = (char*)malloc(sizeof(char) * 1024);
    power = (int*)malloc(sizeof(int) * 2);
    memset(filename, 0, sizeof(char) * 1024);
    memset(power, 0, sizeof(int) * 2);

    map<string, int> mapresult;
    map<string, int>::iterator it;
    vector<string> files;
    vector<string>::iterator itfile;
    readfiledirectory(files, FilePath);
    if (files.size() != 0)
    {
        i = 0;
        itfile = files.begin();
        do {
            printf("------------------------ I am manager ------------------------\n");
            MPI_Recv(power, 2, MPI_INT, MPI_ANY_SOURCE, TAGFORWORKERSATTE, MPI_COMM_WORLD, &status);   // Manager 接收到Worker的请求信息		
//          c = fgetc(fp);
            if (power[0] == 0)
            {
                if ((itfile != files.end()) && (power[1] != 0))
                {
                    printf("I received the request from process :%d\n", power[1]); 		/************ 接收到信息后开始读文件***************/

                    printf("filename %d = %s\n", i, itfile->c_str());
                    i++;
                    /*
                    while ((c != '\n') && (c != EOF))
                    {
                        filename[i] = c;
                        i++;
                        c = fgetc(fp);
                    }
                    */
                    //                memset(filename, 0, sizeof(char) * 1024);
                    strcpy(filename, itfile->c_str());
                    MPI_Send(filename, 1024, MPI_CHAR, power[1], TAGFORTASK, MPI_COMM_WORLD);
                    //               i = 0;
                    printf("I sent the file : %s to the process :%d\n", filename, power[1]);
                    itfile++;
                }
                /**************** 如果文件读完则发送读完的消息给worker **********************/
                else if (itfile == files.end())
                {
                    printf("Now no files need process , send terminate sign to process:%d\n", power[1]);
                    filename[0] = '\n';
                    MPI_Send(filename, 1024, MPI_CHAR, power[1], TAGFORTASK, MPI_COMM_WORLD);
                }
                /******************** worker处理完发送处理完的信号**********************/
            }
            else if (power[0] == 2)    //worker处理完接收worker返回信息
            {
                printf("I received the result from process :%d\n", power[1]);
                int tag = TAGFORRESULT;
                int mlength = 0;
                int freqlen = 0;
                MPI_Recv(&mlength, 1, MPI_INT, power[1], tag++, MPI_COMM_WORLD, &status);
                MPI_Recv(&freqlen, 1, MPI_INT, power[1], tag++, MPI_COMM_WORLD, &status);
                printf("maneger mlength: %d\n", mlength);
                printf("maneger freqlen: %d\n", freqlen);
                int sendtimes = mlength / SEGMENT_LEN;
                int sendsufix = mlength % SEGMENT_LEN;
                char* wordresult = (char*)calloc(mlength, sizeof(char));
                int* freqresult = (int*)calloc(freqlen, sizeof(int));
                //先分片接收单词结果
                for (int i = 0; i < sendtimes; i++)
                {
                    MPI_Recv(wordresult + i * SEGMENT_LEN, SEGMENT_LEN, MPI_CHAR, power[1], tag++, MPI_COMM_WORLD, &status);
//                    tag++;
                }
                //发送剩余的
                MPI_Recv(wordresult + sendtimes * SEGMENT_LEN, sendsufix, MPI_CHAR, power[1], tag++, MPI_COMM_WORLD, &status);
                /*调试*/
//                MPI_Barrier(MPI_COMM_WORLD);
                printf("wordresult: %s\n", wordresult);
//                MPI_Barrier(MPI_COMM_WORLD);

                sendtimes = freqlen / FREQ_LEN;
                sendsufix = freqlen % FREQ_LEN;
                //发送频率结果
                for (int i = 0; i < sendtimes; i++)
                {
                    MPI_Recv(freqresult + i * FREQ_LEN, FREQ_LEN, MPI_INT, power[1], tag++, MPI_COMM_WORLD, &status);
                }
                //发送剩余的
                MPI_Recv(freqresult + sendtimes * FREQ_LEN, sendsufix, MPI_INT, power[1], tag++, MPI_COMM_WORLD, &status);


                /*调试*/
 //               MPI_Barrier(MPI_COMM_WORLD);
                
                for (int i = 0; i < freqlen; i++)
                {
                    printf("%d ", freqresult[i]);
                }
 //               MPI_Barrier(MPI_COMM_WORLD);

                resultrestore(mapresult, wordresult, mlength, freqresult, freqlen);
                printf("*****************test--final map size***********************");
                printf("The final size  of mapresult: %d\n", mapresult.size());
                free(wordresult);
                free(freqresult);
                printf("*****************test***********************");
            }
            else if (power[0] == 1)
            {
                terminate++;
            }
            printf("------------------------------------------------\n\n\n");
        } while (terminate < (p - 1));
    }
    free(filename);
    free(power);
    printf("------------------------ The all work have done ---------------------------\n");
    printf("The result:\n");
    for (it = mapresult.begin(); it != mapresult.end(); it++)
    {
        printf("word: %s-----freq: %d\n", it->first.c_str(), it->second);
    }
    //    fclose(fp);
}
void worker(int id)
{
    int* power;
    char* filename;
    FILE* fp;
    MPI_Status status;
    power = (int*)malloc(sizeof(int) * 2);
    filename = (char*)malloc(sizeof(char) * 1024);
    memset(power, 0, sizeof(int) * 2);
    memset(filename, 0, sizeof(char) * 1024);
    power[0] = 0;
    power[1] = id;
    for (;;)
    {
        printf("-----------------------I am worker : %d -------------------------\n", power[1]);
        MPI_Send(power, 2, MPI_INT, 0, TAGFORWORKERSATTE, MPI_COMM_WORLD);     // 给Manager发送请求信息		
        printf("I have sent request to manager!\n");
        MPI_Recv(filename, 1024, MPI_CHAR, 0, TAGFORTASK, MPI_COMM_WORLD, &status);
        if (filename[0] != '\n')
        {
            printf("I received the file : %s from Manager.\n", filename);
            char* wordresult = NULL;
            int mlength = 0;
            int* freqresult = NULL;
            int freqlen = 0;
            int tag = TAGFORRESULT;
            readfile(filename, wordresult, mlength, freqresult, freqlen);
            if (wordresult == NULL || freqlen == 0)
            {
                printf("空文件\n");
                continue;
            }
            //读完文件之后发送，完成任务信号，然后发送结果
            printf("worker wordresult: %s", wordresult);
            power[0] = 2;
            MPI_Send(power, 2, MPI_INT, 0, TAGFORWORKERSATTE, MPI_COMM_WORLD);
            printf("I will send the result to the manager!\n");
            MPI_Send(&mlength, 1, MPI_INT, 0, tag++, MPI_COMM_WORLD);
            MPI_Send(&freqlen, 1, MPI_INT, 0, tag++, MPI_COMM_WORLD);
            int sendtimes = mlength / SEGMENT_LEN;
            int sendsufix = mlength % SEGMENT_LEN;
            //先分片发送单词结果
            printf("sendtimes = %d------sendsufix = %d \n", sendtimes, sendsufix);
            for (int i = 0; i < sendtimes; i++)
            {
                MPI_Send(wordresult + i * SEGMENT_LEN, SEGMENT_LEN, MPI_CHAR, 0, tag++, MPI_COMM_WORLD);
 //               tag++;
            }
            //发送剩余的
            MPI_Send(wordresult + sendtimes * SEGMENT_LEN, sendsufix, MPI_CHAR, 0, tag++, MPI_COMM_WORLD);
//            printf("manager word result: %s\n", wordresult);
            sendtimes = freqlen / FREQ_LEN;
            sendsufix = freqlen % FREQ_LEN;
            //发送频率结果
            for (int i = 0; i < sendtimes; i++)
            {
                MPI_Send(freqresult + i * FREQ_LEN, FREQ_LEN, MPI_INT, 0, tag++, MPI_COMM_WORLD);
            }
            //发送剩余的
            MPI_Send(freqresult + sendtimes * FREQ_LEN, sendsufix, MPI_INT, 0, tag++, MPI_COMM_WORLD);

            /*
            for (int i = 0; i < freqlen; i++)
            {
                printf("%d ", freqresult[i]);
            }*/
            free(wordresult);
            free(freqresult);
            power[0] = 0;               //更新worker的状态为空闲
        }
        else
        {
            printf("I have not received from Manager , shoud be terminated .\n");
            power[0] = 1;
            MPI_Send(power, 2, MPI_INT, 0, TAGFORWORKERSATTE, MPI_COMM_WORLD);
            break;
        }
        printf("-----------------------------------------------------------------\n\n\n");
    }
    free(filename);
    free(power);
}

int read_file_name()
{
    return 10;
}


// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单
// 入门使用技巧: 
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5. 转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件
