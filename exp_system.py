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
    '2pl-wound-wait'
]

system_branch_map = {
    'splinterdb': 'deukyeon/fantastiCC-refactor',
    'tictoc-disk': 'deukyeon/fantastiCC-refactor',
    'silo-disk': 'deukyeon/silo-disk',
    'baseline-serial': 'deukyeon/baseline',
    'baseline-parallel': 'deukyeon/baseline',
    'silo-memory': 'deukyeon/fantastiCC-refactor',
    'tictoc-memory': 'deukyeon/fantastiCC-refactor',
    'tictoc-counter': 'deukyeon/fantastiCC-refactor',
    'tictoc-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-disk': 'deukyeon/fantastiCC-refactor',
    'sto-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-counter': 'deukyeon/fantastiCC-refactor',
    'sto-memory': 'deukyeon/fantastiCC-refactor',
    '2pl-no-wait': 'deukyeon/fantastiCC-refactor',
    '2pl-wait-die': 'deukyeon/fantastiCC-refactor',
    '2pl-wound-wait': 'deukyeon/fantastiCC-refactor'
}

system_sed_map = {
    'baseline-parallel': ["sed -i 's/\/\/ #define PARALLEL_VALIDATION/#define PARALLEL_VALIDATION/g' src/transaction_private.h"],
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
    '2pl-wound-wait': ["sed -i 's/#define EXPERIMENTAL_MODE_2PL_WOUND_WAIT [ ]*0/#define EXPERIMENTAL_MODE_2PL_WOUND_WAIT 1/g' src/experimental_mode.h"]
}

class ExpSystem:
    @staticmethod
    def build(sys, splinterdb_dir):


        def run_cmd(cmd):
            subprocess.call(cmd, shell=True)
        

        os.environ['CC'] = 'clang'
        os.environ['LD'] = 'clang'
        current_dir = os.getcwd()
        run_cmd(f'tar czf splinterdb-backup-{time.time()}.tar.gz {splinterdb_dir}')
        os.chdir(splinterdb_dir)
        run_cmd('git checkout -- .')
        run_cmd(f'git checkout {system_branch_map[sys]}')
        run_cmd('sudo -E make clean')
        if sys in system_sed_map:
            for sed in system_sed_map[sys]:
                run_cmd(sed)
        run_cmd('sudo -E make install')
        run_cmd('sudo ldconfig')
        os.chdir(current_dir)
        run_cmd('make clean')
        run_cmd('make')
