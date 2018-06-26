#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# Copyright 2016-2018 Workiva Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# system imports
import argparse
import urllib

# library imports

# application imports
from aws_lambda_fsm.config import get_current_configuration
from aws_lambda_fsm.constants import CONFIG

# setup the command line args
parser = argparse.ArgumentParser(description='Turns a fsm.yaml file into a GraphViz URL.')
parser.add_argument('--machine_name')
parser.add_argument('--draw_lambdas', type=bool, default=False)
parser.add_argument('--format', default='uri')
args = parser.parse_args()

LINDEX = 1


def prepend_lambda_to_label(label):
    """
    Adds a "(λ#)" str to the beginning of a str label.

    :param label: a str.
    :return: a str with "(λ#) " prepended
    """
    if args.draw_lambdas:
        global LINDEX
        label = ("(λ%d) " % LINDEX) + label
        LINDEX += 1
    return label


def output_transition_dict(state_dict, transition_dict):
    """
    Outputs a GraphViz directed edge representing an FSM transition.

    :param state_dict: a dict with state info.
    :param transition_dict: a dict with transition info.
    :return: a str.
    """
    label = prepend_lambda_to_label(transition_dict[CONFIG.EVENT])
    if transition_dict.get(CONFIG.ACTION):
        label += '/ ' + transition_dict[CONFIG.ACTION]
    return '"%(source)s" -> "%(target)s" [label="%(label)s"];' % \
           {'source': state_dict[CONFIG.NAME],
            'target': transition_dict[CONFIG.TARGET],
            'label': label}


def output_state_dict(state_dict):
    """
    Outputs a GraphViz node representing an FSM state.

    :param state_dict: a dict with state info.
    :return: a str.
    """
    actions = list()
    if state_dict.get(CONFIG.ENTRY_ACTION):
        actions.append('entry/ %(entry)s' % {'entry': state_dict[CONFIG.ENTRY_ACTION]})
    if state_dict.get(CONFIG.DO_ACTION):
        actions.append('do/ %(do)s' % {'do': state_dict[CONFIG.DO_ACTION]})
    if state_dict.get(CONFIG.EXIT_ACTION):
        actions.append('exit/ %(exit)s' % {'exit': state_dict[CONFIG.EXIT_ACTION]})
    label = '%(state_name)s|%(actions)s' % {'state_name': state_dict[CONFIG.NAME], 'actions': '\\l'.join(actions)}
    shape = 'Mrecord'
    return '"%(state_name)s" [shape=%(shape)s,label="{%(label)s}"];' % \
           {'state_name': state_dict[CONFIG.NAME],
            'shape': shape,
            'label': label}


def output_machine_dict(machine_dict):
    """
    Outputs a GraphViz .dot file representing an FSM.

    :param machine_dict: a dict with machine info.
    :return: a str.
    """
    lines = list()
    lines.append('digraph G {')
    lines.append('label="%(machine_name)s"' % {'machine_name': machine_dict[CONFIG.NAME]})
    lines.append('labelloc="t"')
    lines.append('"__start__" [label="start",shape=circle,style=filled,fillcolor=black,fontcolor=white,fontsize=9];')
    for state_dict in machine_dict.get(CONFIG.STATES, []):
        lines.append(output_state_dict(state_dict))
        if state_dict.get(CONFIG.INITIAL):
            label = prepend_lambda_to_label('')
            lines.append('"__start__" -> "%(state_name)s" [label="%(label)s"]' %
                         {'state_name': state_dict[CONFIG.NAME],
                          'label': label})
        if state_dict.get(CONFIG.FINAL):
            label = prepend_lambda_to_label('')
            lines.append('"%(state_name)s" -> "__end__" [label="%(label)s"]' %
                         {'state_name': state_dict[CONFIG.NAME],
                          'label': label})
        for transition_dict in state_dict.get(CONFIG.TRANSITIONS, []):
            lines.append(output_transition_dict(state_dict, transition_dict))
    lines.append('"__end__" [label="end",shape=doublecircle,style=filled,fillcolor=black,fontcolor=white,fontsize=9];')
    lines.append('}')
    return '\n'.join(lines)


def search_for_machine(filename='fsm.yaml'):
    """
    Searches the .yaml hierarchy for the correct machine.

    :param filename: a path to a fsm.yaml file
    :return:
    """
    for machine_dict in get_current_configuration(filename=filename)[CONFIG.MACHINES]:
        if CONFIG.IMPORT in machine_dict:
            search_for_machine(filename=machine_dict[CONFIG.IMPORT])
            continue
        if machine_dict[CONFIG.NAME] == args.machine_name:
            chl = output_machine_dict(machine_dict)
            if args.format == 'dot':
                print chl
            else:
                print 'https://chart.googleapis.com/chart?cht=gv&chl=%(chl)s' % {'chl': urllib.quote_plus(chl)}

# find the machine in the machine list
search_for_machine()
