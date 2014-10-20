#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <io.h>
#include <share.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/locking.h>
#include <sys/stat.h>

#include "db.h"

static int my_open(char *file)
{
	int hnd;
	hnd=_sopen(file,_O_BINARY|_O_RDWR,_SH_DENYNO,_S_IREAD|_S_IWRITE);
	if(hnd<2)
		exit(printf("OPEN ERROR"));
	return(hnd);
}

static int my_create(char *file)
{
	int hnd;
	hnd=_sopen(file,_O_BINARY|_O_RDWR|_O_TRUNC|_O_CREAT,
					_SH_DENYNO,_S_IREAD|_S_IWRITE);
	if(hnd<2)
		exit(printf("CREATE ERROR"));
	return(hnd);
}

static void my_close(int hnd)
{
	if(_close(hnd)!=0)
		exit(printf("CLOSE ERROR"));
}

static void my_seek(int hnd,long pos)
{
	if(_lseek(hnd,pos,SEEK_SET)==-1)
		exit(printf("SEEK ERROR"));
}

static long my_lof(int hnd)
{
	long size;
	size=_lseek(hnd,0L,SEEK_END);
	if(size==-1)
		exit(printf("LOF ERROR"));
	return(size);
}

static void my_read(int hnd,void *data,int size)
{
	if(_read(hnd,data,size)!=size)
		exit(printf("READ ERROR"));
}

static long my_readl(int hnd)
{
	long data;
	my_read(hnd,&data,4);
	return(data);
}

static void my_write(int hnd,void *data,int size)
{
	if(_write(hnd,data,size)!=size)
		exit(printf("WRITE ERROR"));
}

static void my_writel(int hnd,long data)
{
	my_write(hnd,&data,4);
}

static void my_lock(int hnd,long nbytes)
{
	if(_locking(hnd,_LK_LOCK,nbytes)!=0)
		exit(printf("LOCK ERROR"));
}

static void my_unlock(int hnd,long nbytes)
{
	if(_locking(hnd,_LK_UNLCK,nbytes)!=0)
		exit(printf("UNLOCK ERROR"));
}

static void my_move(int hnd,long fpos,long tpos,long size)
{
	long segment;
	char buff[1024];

	segment=size-fpos;
	if(segment<1)
		return;
	if(fpos<0||tpos<0)
		exit(printf("MOVE ERROR"));
	if(fpos>tpos){
		while(segment>=1024){
			my_seek(hnd,fpos);
			my_read(hnd,buff,1024);
			my_seek(hnd,tpos);
			my_write(hnd,buff,1024);
			fpos+=1024L;
			tpos+=1024L;
			segment-=1024L;
		}
		if(segment>0){
			my_seek(hnd,fpos);
			my_read(hnd,buff,segment);
			my_seek(hnd,tpos);
			my_write(hnd,buff,segment);
		}
	}
	if(fpos<tpos){
		while(segment>=1024){
			my_seek(hnd,fpos+segment-1024);
			my_read(hnd,buff,1024);
			my_seek(hnd,tpos+segment-1024);
			my_write(hnd,buff,1024);
			segment-=1024;
		}
		if(segment>0){
			my_seek(hnd,fpos);
			my_read(hnd,buff,segment);
			my_seek(hnd,tpos);
			my_write(hnd,buff,segment);
		}
	}
}

int db_open(char *file)
{
	long ver;
	int hnd;

	hnd=my_open(file);
	my_seek(hnd,12L);
	ver=my_readl(hnd);
	if(ver!=DB_VER)
		exit(printf("NOT DB FILE"));
	return(hnd);
}

int db_create(char *file,long rln)
{
	long cnt;
	int hnd;

	if(rln<1||rln>DB_MAXRLN)
		exit(printf("DB INVALID RECLN"));

	hnd=my_create(file);
	my_writel(hnd,0L);
	my_writel(hnd,rln);
	my_writel(hnd,DB_MAXREC);
	my_writel(hnd,DB_VER);
	for(cnt=0;cnt<DB_MAXREC;cnt++)
		my_writel(hnd,0L);
	return(hnd);
}

void db_close(int hnd)
{
	my_close(hnd);
}

int db_read(int hnd,char *key,void *data)
{
	long mid,low,high,pos,rcln;
	char tkey[DB_KEYLEN];
	int res;

	low=0;
	my_seek(hnd,0L);
	high=my_readl(hnd)-1L;
	if(high<0)
		return(0);
	rcln=my_readl(hnd);
	while(low<=high){
		mid=(low+high)/2;
		my_seek(hnd,mid*4L+16L);
		pos=my_readl(hnd);
		my_seek(hnd,pos);
		my_read(hnd,tkey,DB_KEYLEN);
		res=stricmp(key,tkey);
		if(res==0){
			my_read(hnd,data,rcln);
			return(1);
		}
		if(res>0)
			low=mid+1;
		else
			high=mid-1;
	}
	return(0);
}

int db_next(int hnd,char *key,void *data)
{
	long mid,low,high,pos,rcln,cnt;
	char tkey[DB_KEYLEN];
	int res;

	low=0;
	my_seek(hnd,0L);
	cnt=my_readl(hnd);
	if(cnt<1)
		return(0);
	high=cnt-1;
	rcln=my_readl(hnd);
	if(strlen(key)==0){
		my_seek(hnd,16L);
		pos=my_readl(hnd);
		my_seek(hnd,pos);
		my_read(hnd,key,DB_KEYLEN);
		my_read(hnd,data,rcln);
		return(1);
	}
	while(low<=high){
		mid=(low+high)/2;
		my_seek(hnd,mid*4L+16L);
		pos=my_readl(hnd);
		my_seek(hnd,pos);
		my_read(hnd,tkey,DB_KEYLEN);
		res=stricmp(key,tkey);
		if(res==0){
			if(mid==cnt-1)
				return(0);
			mid++;
			my_seek(hnd,mid*4L+16L);
			pos=my_readl(hnd);
			my_seek(hnd,pos);
			my_read(hnd,key,DB_KEYLEN);
			my_read(hnd,data,rcln);
			return(1);
		}
		if(res>0)
			low=mid+1;
		else
			high=mid-1;
	}
	return(0);
}

int db_write(int hnd,char *key,void *data)
{
	long mid,low,high,pos,rln,cnt,mrc;
	char tkey[DB_KEYLEN];
	int res;

	my_seek(hnd,0L);
	my_lock(hnd,4L);
	low=0;
	cnt=my_readl(hnd);
	high=cnt-1;
	rln=my_readl(hnd);
	mrc=my_readl(hnd);
	while(low<=high){
		mid=(low+high)/2;
		my_seek(hnd,mid*4L+16L);
		pos=my_readl(hnd);
		my_seek(hnd,pos);
		my_read(hnd,tkey,DB_KEYLEN);
		res=stricmp(key,tkey);
		if(res==0){
			my_write(hnd,data,rln);
			my_seek(hnd,0);
			my_unlock(hnd,4L);
			return(1);
		}
		if(res>0)
			low=mid+1;
		else
			high=mid-1;
	}
	if(cnt>=mrc)
		exit(printf("DB FULL"));
	if(low<cnt)
		my_move(hnd,low*4L+16L,low*4L+20L,cnt*4L+16L);
	pos=my_lof(hnd);
	my_seek(hnd,low*4L+16L);
	my_writel(hnd,pos);
	my_seek(hnd,pos);
	if(strlen(key)>=DB_KEYLEN)
		key[DB_KEYLEN-1]=0;
	memset(tkey,0,DB_KEYLEN);
	strcpy(tkey,key);
	my_write(hnd,tkey,DB_KEYLEN);
	my_write(hnd,data,rln);
	cnt++;
	my_seek(hnd,0L);
	my_writel(hnd,cnt);
	my_seek(hnd,0L);
	my_unlock(hnd,4L);
	return(0);
}

int db_delete(int hnd,char *key)
{
	long mid,low,high,pos,cnt;
	char tkey[DB_KEYLEN];
	int res;

	my_seek(hnd,0L);
	my_lock(hnd,4L);
	low=0;
	cnt=my_readl(hnd);
	high=cnt-1;
	while(low<=high){
		mid=(low+high)/2;
		my_seek(hnd,mid*4L+16L);
		pos=my_readl(hnd);
		my_seek(hnd,pos);
		my_read(hnd,tkey,DB_KEYLEN);
		res=stricmp(key,tkey);
		if(res==0){
			if(mid<cnt)
				my_move(hnd,mid*4L+20L,mid*4L+16L,cnt*4L+16L);
			cnt--;
			my_seek(hnd,0L);
			my_writel(hnd,cnt);
			my_seek(hnd,0L);
			my_unlock(hnd,4L);
			return(1);
		}
		if(res>0)
			low=mid+1;
		else
			high=mid-1;
	}
	my_seek(hnd,0L);
	my_unlock(hnd,4L);
	return(0);
}
