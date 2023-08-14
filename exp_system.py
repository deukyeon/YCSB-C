import os

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
    'sto-sketch',
    'sto-counter',
    'sto-memory',
    '2pl'
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
    'sto-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-counter': 'deukyeon/fantastiCC-refactor',
    'sto-memory': 'deukyeon/fantastiCC-refactor',
    '2pl': 'deukyeon/fantastiCC-refactor'
}

system_sed_map = {
    'baseline-parallel': ["sed -i 's/\/\/ #define PARALLEL_VALIDATION/#define PARALLEL_VALIDATION/g' src/transaction_private.h"],
    'silo-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_SILO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_SILO_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_DISK [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_DISK 1/g' src/experimental_mode.h"],
    'tictoc-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_COUNTER 1/g' src/experimental_mode.h"],
    'tictoc-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_SKETCH 1/g' src/experimental_mode.h"],
    'sto-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_STO_MEMORY 1/g' src/experimental_mode.h"],
    'sto-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_STO_SKETCH 1/g' src/experimental_mode.h"],
    'sto-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_STO_COUNTER 1/g' src/experimental_mode.h"],
    '2pl': ["sed -i 's/#define EXPERIMENTAL_MODE_2PL [ ]*0/#define EXPERIMENTAL_MODE_2PL 1/g' src/experimental_mode.h"],
}

class ExpSystem:
    @staticmethod
    def build(sys, splinterdb_dir):
        os.environ['CC'] = 'clang'
        os.environ['LD'] = 'clang'
        current_dir = os.getcwd()
        backup_file = 'splinterdb-backup.tar.gz'
        if os.file.exists(backup_file):
            os.remove(backup_file)
        os.system(f'tar czf splinterdb-backup.tar.gz {splinterdb_dir}')
        os.chdir(splinterdb_dir)
        os.system('git checkout -- .')
        os.system(f'git checkout {system_branch_map[sys]}')
        os.system('sudo -E make clean')
        if sys in system_sed_map:
            for sed in system_sed_map[sys]:
                os.system(sed)
        os.system('sudo -E make install')
        os.system('sudo ldconfig')
        os.chdir(current_dir)
        os.system('make clean')
        os.system('make')
