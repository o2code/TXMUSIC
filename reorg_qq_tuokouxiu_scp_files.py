import os,sys
from tqdm import tqdm
import math
import multiprocessing
import os,sys
import soundfile as sf
from tqdm import tqdm

def check_tsec_core(datadir, itemlst, fnok, fnerr):
    lst_err = []
    lst_ok = []
    for item in itemlst:
        arr = item.strip().split('\t')
        if len(arr) != 5:
            #print(f'item not 5 cols: {arr}')
            continue
        ftag, fpath, txt, spkname, tsec = arr
        tsec = float(tsec)
        fnwav = f'{datadir}/{fpath}'
        x, fs = sf.read(fnwav)
        if len(x) == 0:
            lst_err.append([ftag, fpath, txt, spkname, tsec])
        else:
            lst_ok.append([ftag, fpath, txt, spkname, tsec])

    if len(lst_ok) > 0:
        with open(fnok, 'w') as fw:
            for item in lst_ok:
                ftag, fpath, txt, spkname, tsec = item
                fw.write(f'{ftag}\t{fpath}\t{txt}\t{spkname}\t{tsec}\n')
    if len(lst_err) > 0:
        with open(fnerr, 'w') as fw:
            for item in lst_err:
                ftag, fpath, txt, spkname, tsec = item
                fw.write(f'{ftag}\t{fpath}\t{txt}\t{spkname}\t{tsec}\n')

def check_tsec(datadir,fnin,fnok,fnerr,fnsmall):
    with open(fnin) as fr:
        lines = fr.readlines()
    check_tsec_core(datadir,lines,fnok,fnerr,fnsmall)
    

def check_tsec_1(datadir, fnin, fnok, fnerr):
    with open(fnin) as fr:
        lines = fr.readlines()
    nproc = 80
    na = len(lines)
    subsize = math.ceil(na / nproc)
    fnlst = [lines[i:i+subsize] for i in range(0, na, subsize)]
    processlst = []
    for idx in range(nproc):
        fnerr = f'{fnerr}_{idx}.txt'
        fnok = f'{fnok}_{idx}.txt'
        processlst.append(multiprocessing.Process(target=check_tsec_core, args=(datadir, fnlst[idx], fnok, fnerr)))
        processlst[-1].start()  # 启动新创建的进程
        
    # for p in processlst:
    #     p.start()

    for p in processlst:
        p.join()

if __name__ == '__main__':
    datadir = '/data/home/pennyczhang/dbtts_disk2/zhongyuan/qq_tingshu/acg/denoised_cutted_44100'
    split_dir = f'{datadir}/split_scps/raw_split_set/'
    scp_outdir = f'{datadir}/split_scps/final_split_set/'
    if not os.path.exists(scp_outdir):
        os.makedirs(scp_outdir)
    if True:  # 多进程
        nproc = 111
        processlst = []
        for indx in range(1, 2):
            fnscp = f'{split_dir}/dbset{indx}.scp'
            dbset = 'dbset%d' % (indx)
            for idx in range(nproc):
                fnok = f'{scp_outdir}/ok/{dbset}_proc{idx}.scp'
                fnerr = f'{scp_outdir}/err/{dbset}__proc{idx}.scp'
                processlst.append(multiprocessing.Process(target=check_tsec, args=(datadir, fnscp, fnok, fnerr)))
            for p in processlst:
                p.start()

            for p in processlst:
                p.join()
