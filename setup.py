from setuptools import find_packages,setup
## Metadata Information about the project

HYPEN_E_DOT='-e .'
def get_requirements(file_path:str)->list[str]:
    '''
    this function will return the list of required libraries from the file provided
    '''
    requirements=[]
    with open(file_path) as file_obj:
        requirements=file_obj.readlines()
        requirements=[req.replace("\n","") for req in requirements]

        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
    print(requirements)
    return requirements
    
setup(
name='DM4ML_Assignment_Group_3_Project_Repo',
version='0.0.1',
author='Abhinav Sood (BITS_ID# 2023dc04233), Gaurav Parmanandka (BITS_ID# 2023dc04060), Liza Bini Stephen (BITS_ID# 2023dc04061), Prithviraj Acharya (BITS_ID# 2023dc04009)',
author_email='2023dc04233@wilp.bits-pilani.ac.in, 2023dc04060@wilp.bits-pilani.ac.in, 2023dc04061@wilp.bits-pilani.ac.in, 2023dc04009@wilp.bits-pilani.ac.in',
packages=find_packages(), ##searches __init__.py file in all directories and try to build that as a package
##install_requires=['pandas','numpy','matplotlib','seaborn','scikit-learn','jupyter','jupyterlab']
install_requires=get_requirements('requirements.txt') ## To fetch the library requirements from the requirements.txt file
)

