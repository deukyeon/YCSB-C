import os
import time
import subprocess

available_systems = [
    'splinterdb',
    'tictoc-disk',
    'silo-disk',
    'baseline-serial',
    'baseline-parallel',
    'silo-memory',
    'tictoc-memory',
    'tictoc-counter',
    'tictoc-sketch',
    'sto-disk',
    'sto-sketch',
    'sto-counter',
    'sto-memory',
    '2pl-no-wait',
    '2pl-wait-die',
    '2pl-wound-wait',
    'mvcc-disk',
    'mvcc-memory',
    'mvcc-counter',
    'mvcc-sketch'
]

system_branch_map = {
    'splinterdb': 'deukyeon/mvcc-working-merge-main',
    'tictoc-disk': 'deukyeon/mvcc-working-merge-main',
    'silo-disk': 'deukyeon/mvcc-working-merge-main',
    'baseline-serial': 'deukyeon/mvcc-working-merge-main',
    'baseline-parallel': 'deukyeon/mvcc-working-merge-main',
    'silo-memory': 'deukyeon/mvcc-working-merge-main',
    'tictoc-memory': 'deukyeon/mvcc-working-merge-main',
    'tictoc-counter': 'deukyeon/mvcc-working-merge-main',
    'tictoc-sketch': 'deukyeon/mvcc-working-merge-main',
    'sto-disk': 'deukyeon/mvcc-working-merge-main',
    'sto-sketch': 'deukyeon/mvcc-working-merge-main',
    'sto-counter': 'deukyeon/mvcc-working-merge-main',
    'sto-memory': 'deukyeon/mvcc-working-merge-main',
    '2pl-no-wait': 'deukyeon/mvcc-working-merge-main',
    '2pl-wait-die': 'deukyeon/mvcc-working-merge-main',
    '2pl-wound-wait': 'deukyeon/mvcc-working-merge-main',
    'mvcc-disk': 'deukyeon/mvcc-working-merge-main',
    # 'mvcc-disk': 'robj/fantastiCC-refactor-bugfix',
    'mvcc-memory': 'deukyeon/mvcc-working-merge-main',
    'mvcc-counter': 'deukyeon/mvcc-working-merge-main',
    'mvcc-sketch': 'deukyeon/mvcc-working-merge-main',
}

system_sed_map = {
    'baseline-serial': ["sed -i 's/#define EXPERIMENTAL_MODE_KR_OCC [ ]*0/#define EXPERIMENTAL_MODE_KR_OCC 1/g' src/experimental_mode.h"],
    'baseline-parallel': ["sed -i 's/#define EXPERIMENTAL_MODE_KR_OCC_PARALLEL [ ]*0/#define EXPERIMENTAL_MODE_KR_OCC_PARALLEL 1/g' src/experimental_mode.h"],
    'silo-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_SILO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_SILO_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_DISK [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_DISK 1/g' src/experimental_mode.h"],
    'tictoc-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_COUNTER 1/g' src/experimental_mode.h"],
    'tictoc-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_SKETCH 1/g' src/experimental_mode.h"],
    'sto-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_DISK [ ]*0/#define EXPERIMENTAL_MODE_STO_DISK 1/g' src/experimental_mode.h"],
    'sto-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_STO_MEMORY 1/g' src/experimental_mode.h"],
    'sto-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_STO_SKETCH 1/g' src/experimental_mode.h"],
    'sto-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_STO_COUNTER 1/g' src/experimental_mode.h"],
    '2pl-no-wait': ["sed -i 's/#define EXPERIMENTAL_MODE_2PL_NO_WAIT [ ]*0/#define EXPERIMENTAL_MODE_2PL_NO_WAIT 1/g' src/experimental_mode.h"],
    '2pl-wait-die': ["sed -i 's/#define EXPERIMENTAL_MODE_2PL_WAIT_DIE [ ]*0/#define EXPERIMENTAL_MODE_2PL_WAIT_DIE 1/g' src/experimental_mode.h"],
    '2pl-wound-wait': ["sed -i 's/#define EXPERIMENTAL_MODE_2PL_WOUND_WAIT [ ]*0/#define EXPERIMENTAL_MODE_2PL_WOUND_WAIT 1/g' src/experimental_mode.h"],
    'mvcc-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_MVCC_DISK [ ]*0/#define EXPERIMENTAL_MODE_MVCC_DISK 1/g' src/experimental_mode.h"],
    'mvcc-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_MVCC_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_MVCC_MEMORY 1/g' src/experimental_mode.h"],
    'mvcc-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_MVCC_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_MVCC_COUNTER 1/g' src/experimental_mode.h"],
    'mvcc-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_MVCC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_MVCC_SKETCH 1/g' src/experimental_mode.h"],
}

class ExpSystem:
    @staticmethod
    def build(sys, splinterdb_dir, backup=True):


        def run_cmd(cmd):
            subprocess.call(cmd, shell=True)
        

        os.environ['CC'] = 'clang'
        os.environ['LD'] = 'clang'
        current_dir = os.getcwd()
        if backup:
            run_cmd(f'tar czf splinterdb-backup-{time.time()}.tar.gz {splinterdb_dir}')
        os.chdir(splinterdb_dir)
        run_cmd('git checkout -- src/experimental_mode.h')
        run_cmd(f'git checkout {system_branch_map[sys]}')
        run_cmd('sudo -E make clean')
        if sys in system_sed_map:
            for sed in system_sed_map[sys]:
                run_cmd(sed)
        run_cmd('sudo -E make -j16 install')
        run_cmd('sudo ldconfig')
        os.chdir(current_dir)
        run_cmd('make clean')
        run_cmd('make')
