"""Pipeline logging."""

import cli


def jobStart(funcName):
    """Output "job start" log msg."""
    msg = getFuncDesc(funcName)

    op = '\n'
    op += '------------------------------------------------------------' + '\n'
    op += '|  \n'
    op += '|  Starting ' + msg + "\n"
    op += '|  \n'
    op += '------------------------------------------------------------' + '\n'

    print(op)


def jobEnd(funcName, warnings):
    """Output "job end" log msg."""
    msg = getFuncDesc(funcName)

    op = '\n'
    op += '------------------------------------------------------------' + '\n'
    op += '|  \n'
    op += '|  Finished ' + msg + ', '
    op += str(len(set(warnings))) + ' warnings' + "\n"
    op += '|  \n'
    op += '------------------------------------------------------------' + '\n'

    i = 0
    if len(warnings) > 0:
        op += '\n'
    for warning in set(warnings):
        i += 1
        op += str(i) + '. ' + warning + '\n'

    print(op)


def info(msg):
    """Output generic info message"""
    op = ''
    op += msg
    print(op)


def getFuncDesc(funcName):
    """Output "function description" log msg."""
    try:
        msg = cli.FUNC_NAMES[funcName]
    except KeyError:
        msg = funcName
    return msg
