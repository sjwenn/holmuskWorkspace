'''
This is a modified copy of /src/workspace.py, meant to service the 
loading of modules within this collection of modules.

Any required documentation can be found in /src/workspace.py.
'''

import jsonref, argparse

from importlib      import util
from logs           import logDecorator  as lD
from lib.testLib    import simpleLib     as sL
from lib.argParsers import addAllParsers as aP

config   = jsonref.load(open('../config/config.json'))
logBase  = config['logging']['logBase']
logLevel = config['logging']['level']
logSpecs = config['logging']['specs']

@lD.log(logBase + '.importModules')
def importModules(logger, resultsDict):
    modules = jsonref.load(open('../config/modules/RWEWidgets/modules.json'))
    moduleSetting = jsonref.load(open('../config/modules/RWEWidgets/RWEWidgets.json'))

    # update modules in the right order. Also get rid of the frivilous
    # modules
    if resultsDict['modules'] is not None:
        tempModules = []
        for m in resultsDict['modules']:
            toAdd = [n for n in modules if n['moduleName'] == m][0]
            tempModules.append( toAdd )

        modules = tempModules

     #or moduleSetting['runAll']

    for m in modules:

        if (resultsDict['modules'] is None):

            try:
                if not m['execute'] and not moduleSetting['runAll']: # How does execute=false modules get removed? Magic!
                    logger.info('Module {} is being skipped'.format(m['moduleName']))
                    continue
            except Exception as e:
                logger.error(f'Unable to check whether module the module should be skipped: {e}')
                logger.error(f'this module is being skipped')
                continue

        else:

            # skip based upon CLI
            try:
                if m['moduleName'] not in resultsDict['modules']:
                    logger.info(f'{m} not present within the list of CLI modules. Module is skipped')
                    continue
            except Exception as e:
                logger.error(f'Unable to determine whether this module should be skipped: {e}.\n Module is being skipped.')
                continue


        try:
            name, path = m['moduleName'], m['path']
            logger.info('Module {} is being executed'.format( name ))

            module_spec = util.spec_from_file_location(
                name, path)
            module = util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
            module.main(resultsDict)
        except Exception as e:
            print('Unable to load module: {}->{}\n{}'.format(name, path, str(e)))

    return

@lD.log(logBase + 'RWEWidgetsMain')
def main(logger, resultsDict):

    # First import all the modules, and run 
    # them
    # ------------------------------------
    importModules(resultsDict)

    # Lets just create a simple testing 
    # for other functions to follow
    # -----------------------------------

    return

if __name__ == '__main__':

    # Let us add an argument parser here
    parser = argparse.ArgumentParser(description='workspace command line arguments')
    
    # Add the modules here
    modules = jsonref.load(open('../config/modules/RWEWidgets/RWEWidgets.json'))
    modules = [m['moduleName'] for m in modules]
    parser.add_argument('-m', '--module', action='append',
        type = str,
        choices = modules,
        help = '''Add modules to run over here. Multiple modules can be run
        simply by adding multiple strings over here. Make sure that the 
        available choices are reflected in the choices section''')

    parser = aP.parsersAdd(parser)
    results = parser.parse_args()
    resultsDict = aP.decodeParsers(results)

    if results.module is not None:
        resultsDict['modules'] = results.module
    else:
        resultsDict['modules'] = None
        

    # ---------------------------------------------------
    # We need to explicitely define the logging here
    # rather than as a decorator, bacause we have
    # fundamentally changed the way in which logging 
    # is done here
    # ---------------------------------------------------
    logSpecs = aP.updateArgs(logSpecs, resultsDict['config']['logging']['specs'])
    try:
        logLevel = resultsDict['config']['logging']['level']
    except Exception as e:
        print('Logging level taking from the config file: {}'.format(logLevel))

    logInit  = lD.logInit(logBase, logLevel, logSpecs)
    main     = logInit(main)

    main(resultsDict)
