
# Forked from https://raw.githubusercontent.com/alexdriedger/SlayTheSpireFightPredictor/master/main.py

import os
import gzip
import json
from functools import partial
from collections import Counter
import time
import traceback
import gc
import sys
import re
import itertools
import datetime
import collections
import game_constants
import trial_counter
import shutil
import pprint

class ProcessingProblemsSummary:
    def __init__(self):
        TC = trial_counter.TrialCounter
        self.problems_by_card = collections.defaultdict(TC)
        self.problems_by_relic = collections.defaultdict(TC)
        self.problems_by_event = collections.defaultdict(TC)
        self.problems_by_neow = collections.defaultdict(TC)
        
        #success_count_by_max_floor = collections.Counter()
        #fail_count_by_max_floor = collections.Counter()
        # for k in sorted(set(itertools.chain(success_count_by_max_floor.keys(), fail_count_by_max_floor.keys()))):
        # success = success_count_by_max_floor.get(k, 0)
        # fail = fail_count_by_max_floor.get(k, 0 )
        # print(k, success, fail, success / (success + fail))

    def record_processing_outcome(self, game_data, is_successful):
        for relic in game_data['relics']:
            self.problems_by_relic[relic].record_outcome(is_successful)
        for card in set(game_data['master_deck']):
            self.problems_by_card[card].record_outcome(is_successful)
        for event in game_data['event_choices']:
            self.problems_by_event[event['event_name']].record_outcome(is_successful)
        self.problems_by_neow[game_data['neow_bonus']].record_outcome(is_successful)

    def print_info(self):
        def most_failing_conditions(trial_dict):
            return sorted((a for a in trial_dict.items() if a[1].total > 20),
                          key=lambda item: item[1].success_rate())[:20]
        print('card processing problems',  most_failing_conditions(self.problems_by_card))
        print('relics processing problems', most_failing_conditions(self.problems_by_relic))
        print('event processing problems', most_failing_conditions(self.problems_by_event))
        print('new proccessing problems',  most_failing_conditions(self.problems_by_neow))

        

def process_runs(data_dir):
    file_not_opened = 0
    bad_run_count = 0
    total_file_count = 0
    total_game_count = 0
    run_not_processed_count = 0
    run_processed_count = 0
    run_master_not_match_count = 0
    runs_with_examples = list()
    run_skipped_snobby = 0
    total_runs_written = 0
    total_fights_written = 0
    processing_problems_summary = ProcessingProblemsSummary()
    

    tmp_dir = os.path.join('processed_logs/a20_act1_defect')
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.mkdir(tmp_dir)
    # TODO: delete json data in processed logs dir before writing new data.
    for root, dirs, files in os.walk(data_dir):
        print(root)
        #for fname in files[0:10]:  # hack, remove
        for fname in files:
            path = os.path.join(root, fname)
            if path.endswith("json.gz"):
                
                # Save batch to file
                if len(runs_with_examples) > 500:
                    print('Saving batch')
                    write_file_name = f'data_{round(time.time())}.json'
                    write_file(runs_with_examples, os.path.join(tmp_dir, write_file_name))
                    total_runs_written += len(runs_with_examples)
                    total_fights_written += sum(len(run['fights']) for run in runs_with_examples)
                    runs_with_examples.clear()
                    print('Wrote batch to file')
                    print('Garbage collecting')
                    print(f'Number of total training examples: {total_fights_written}')
                    gc.collect()
                    print('Finished garbage collecting')

                # Garbage collect to improve performance
                if total_game_count % 5000 == 0:
                    gc.collect()

                # Print update
                if total_game_count % 200 == 0 and total_game_count > 0:
                    print(
                        f'\n\n\nFiles not able to open: {file_not_opened} => {((file_not_opened / total_game_count) * 100):.3f} %')
                    print(
                        f'Runs filtered with pre-filter: {bad_run_count} => {((bad_run_count / total_game_count) * 100):.3f} %')
                    print(f'run skipped snobby: {run_skipped_snobby} => {((run_skipped_snobby / total_game_count) * 100):.3f} %')
                    print(
                        f'Runs SUCCESSFULLY processed: {run_processed_count} => {((run_processed_count / total_game_count) * 100):.3f} %')
                    print(
                        f'Runs with master deck not matching created deck: {run_master_not_match_count} => {((run_master_not_match_count / total_game_count) * 100):.3f} %')
                    print(
                        f'Runs not processed: {run_not_processed_count} => {((run_not_processed_count / total_game_count) * 100):.3f} %')
                    print(f'Total runs: {total_game_count}')
                    print(f'Number of runs in batch: {len(runs_with_examples)}')
                    print(f'Number of total training examples: {total_fights_written}')

                # Process file
                try:
                    with gzip.open(path, 'r') as file:
                        for outer_game_data in json.load(file):
                            single_game_data = outer_game_data['event']
                            total_game_count += 1

                            if not (single_game_data['ascension_level'] == 20 and
                                    single_game_data['floor_reached'] >= 15 and
                                    single_game_data['character_chosen'] == 'DEFECT'):
                                run_skipped_snobby += 1
                                continue
                            
                            if is_bad_game_data(single_game_data):
                                bad_run_count += 1
                            else:
                                if 'ReplayTheSpireMod:Calculation Training+1' in single_game_data['master_deck']:
                                    print('Modded run found')
                                    # print(single_game_data)
                                    #print(path)
                                try:
                                    processed_run = process_run(single_game_data, processing_problems_summary)
                                    run_processed_count += 1

                                    runs_with_examples.append(processed_run)
                                except RuntimeError as e:
                                    run_master_not_match_count += 1
                                    # print(f'{path}\n')
                                    pass
                                except Exception as e:
                                    traceback.print_exc(file=sys.stdout)
                                    run_not_processed_count += 1
                                    # print(path)
                except Exception as e:
                    print("file not opened", e)
                    file_not_opened += 1

    write_file_name = f'data_{round(time.time())}.json'
    total_runs_written += len(runs_with_examples)
    total_fights_written += sum(len(runs['fights']) for runs in runs_with_examples)
    write_file(runs_with_examples, os.path.join(tmp_dir, write_file_name))
                                
    print(f'\n\n\nFiles not able to open: {file_not_opened} => {((file_not_opened / total_game_count) * 100):.3f} %')
    print(f'Runs skipped snobby: {run_skipped_snobby} => {((run_skipped_snobby / total_game_count) * 100):.3f} %')
    print(f'Runs filtered with pre-filter: {bad_run_count} => {((bad_run_count / total_game_count) * 100):.3f} %')
    print(f'Runs SUCCESSFULLY processed: {run_processed_count} => {((run_processed_count / total_game_count) * 100):.3f} %')
    print(f'Runs not processed: {run_not_processed_count} => {((run_not_processed_count / total_game_count) * 100):.3f} %')
    print(f'Runs with master deck not matching created deck: {run_master_not_match_count} => {((run_master_not_match_count / total_game_count) * 100):.3f} %')
    print(f'Total runs seen: {total_game_count}')
    print(f'Number of total runs retained: {total_runs_written}')
    print(f'Number of total training examples: {total_fights_written}')
                                
    processing_problems_summary.print_info()
    


def process_run(data, processing_problems_summary):
    battle_stats_by_floor = {battle_stat['floor']: battle_stat for battle_stat in data['damage_taken']}
    events_by_floor = {event_stat['floor']: event_stat for event_stat in data['event_choices']}
    card_choices_by_floor = {card_choice['floor']: card_choice for card_choice in data['card_choices']}
    relics_by_floor = get_relics_by_floor(data)
    campfire_choices_by_floor = {campfire_choice['floor']: campfire_choice for campfire_choice in
                                 data['campfire_choices']}
    purchases_by_floor = get_stat_with_separate_floor_list(data, 'items_purchased', 'item_purchase_floors')
    purges_by_floor = get_stat_with_separate_floor_list(data, 'items_purged', 'items_purged_floors')
    potion_use_by_floor = list(set(data['potions_floor_usage']))

    current_deck = get_starting_deck(data['character_chosen'], data['ascension_level'])
    current_relics = get_starting_relics(data['character_chosen'])

    unknown_removes_by_floor = dict()
    unknown_upgrades_by_floor = dict()
    unknown_transforms_by_floor = dict()
    unknown_cards_by_floor = dict()
    unknowns = (unknown_removes_by_floor, unknown_upgrades_by_floor, unknown_transforms_by_floor, unknown_cards_by_floor)

    processed_fights = list()
    
    def get_next_boss(act):
        if act > 2:
            return 
        next_boss_floor = game_constants.BOSS_FLOORS[act]
        if next_boss_floor in battle_stats_by_floor:
            boss = battle_stats_by_floor[next_boss_floor]['enemies']
            if boss not in game_constants.BOSSES:
                raise RuntimeError('invalid boss '+ str(boss))
            return boss

    last_elite = None
    act = 0  # 0 based act
    act_boss = get_next_boss(act)
    
    for floor in range(0, data['floor_reached'] + 1):
        if floor in game_constants.BOSS_FLOORS:
            last_elite = None
            act += 1
            act_boss = get_next_boss(act)

        if floor in battle_stats_by_floor and floor != 1:
            if battle_stats_by_floor[floor]['enemies'] in game_constants.ELITES:
                last_elite = battle_stats_by_floor[floor]['enemies']
            
            fight_data = process_battle(data, battle_stats_by_floor[floor], potion_use_by_floor, card_choices_by_floor, current_deck, current_relics, floor, last_elite, act_boss)
            processed_fights.append(fight_data)

        if floor in relics_by_floor:
            process_relics(relics_by_floor[floor], current_relics, data['relics'], floor, unknowns, current_deck)

        if floor in card_choices_by_floor:
            process_card_choice(card_choices_by_floor[floor], current_deck, current_relics)

        if floor in campfire_choices_by_floor:
            restart_needed, new_data = try_process_data(partial(process_campfire_choice, campfire_choices_by_floor[floor], current_deck), floor, current_deck, current_relics, data, unknowns)
            if restart_needed:
                return process_run(new_data)

        if floor in purchases_by_floor:
            try_process_data(partial(process_purchases, purchases_by_floor[floor], current_deck, current_relics, data['relics'], floor, unknowns), floor, current_deck, current_relics, data, unknowns)

        if floor in purges_by_floor:
            try_process_data(partial(process_purges, purges_by_floor[floor], current_deck), floor, current_deck, current_relics, data, unknowns)

        if floor in events_by_floor:
            try_process_data(partial(process_events, events_by_floor[floor], current_deck, current_relics, data['relics'], floor, unknowns), floor, current_deck, current_relics, data, unknowns)

        if floor == 0:
            process_neow(data['neow_bonus'], data['neow_cost'], current_deck, current_relics, data['relics'], unknowns)

    current_deck.sort()
    master_deck = sorted(data['master_deck'])
    current_relics.sort()
    master_relics = sorted(data['relics'])
    if current_deck != master_deck or current_relics != master_relics:
        success, new_data = resolve_missing_data(current_deck, current_relics, master_deck=data['master_deck'],
                                                 master_relics=data['relics'], unknowns=unknowns, master_data=data)
        if success:
            return process_run(new_data, processing_problems_summary)
        if current_deck == master_deck:
            pass
            # print(f'\nSo close!!!!!   XX Relics XX')
        elif current_relics == master_relics:
            pass
            #print(f'\nSo close!!!!!   XX Deck XX')
        else:
            pass
            # print(f'\nLess close!!!!!   XX Deck and Relics XX')
        
        
        # print(f'Current Deck\t: {sorted(current_deck)}')
        # print(f'Master Deck\t\t: {sorted(master_deck)}')
        # print(f'Master Deck - Current Deck\t\t: {set(master_deck) - set(current_deck)}')
        # print(f'Current Deck - Master Deck\t\t: {set(current_deck) - set(master_deck)}')
        # print(f'Current Relics\t: {sorted(current_relics)}')
        # print(f'Master Relics\t: {sorted(master_relics)}\n')
        # print(f'Master Relics - Current Relics\t\t: {set(master_deck) - set(current_deck)}')
        # print(f'Current Relics - Master Relics\t\t: {set(current_relics) - set(master_relics)}')
        processing_problems_summary.record_processing_outcome(data, False)
        raise RuntimeError('Final decks or relics did not match')
    else:
        processing_problems_summary.record_processing_outcome(data, True)
        processed_run = {'fights': processed_fights,
                         'player_experience': data['player_experience'],
                         'floor_reached': data['floor_reached'],
                         #'character': master_data['character_chosen'],
                         #'ascension': master_data['ascension_level']
        }
        return processed_run


def try_process_data(func, floor, current_deck, current_relics, master_data, unknowns):
    try:
        func()
        return False, None
    except Exception as e:
        # success, new_data = resolve_missing_data(current_deck, current_relics, master_deck=master_data['master_deck'], master_relics=master_data['relics'], unknowns=unknowns, master_data=master_data)
        # if success:
        #     return success, new_data
        # else:
        floor_reached = master_data['floor_reached']
        master_deck = master_data['master_deck']
        #print(f'\nFunction {func.func.__name__} failed on floor {floor} of {floor_reached}')
        #print(f'Reason for exception: {e}')
        #print(f'Current Deck\t: {sorted(current_deck)}')
        #print(f'Master Deck\t\t: {sorted(master_deck)}\n')
        # pprint.pprint(master_data)
        raise e


def process_battle(master_data, battle_stat, potion_use_by_floor, card_choices_by_floor,
                   current_deck, current_relics,
                   floor, last_elite, act_boss):
    if battle_stat['enemies'] not in game_constants.BASE_GAME_ENEMIES:
        raise RuntimeError('Modded enemy')

    fight_data = dict()
    fight_data['cards'] = list(current_deck)
    fight_data['relics'] = list(current_relics)
    fight_data['max_hp'] = master_data['max_hp_per_floor'][floor - 2]
    fight_data['entering_hp'] = master_data['current_hp_per_floor'][floor - 2]
    fight_data['enemies'] = battle_stat['enemies']
    fight_data['potion_used'] = floor in potion_use_by_floor
    fight_data['floor'] = floor
    fight_data['last_elite'] = last_elite
    fight_data['act_boss'] = act_boss

    if floor not in card_choices_by_floor:
        pass
        # print('missing card choice? floor is', floor, 'enemies is', battle_stat['enemies'])
        # pprint.pprint(master_data)
        # pprint.pprint(battle_stat)
        # pprint.pprint(card_choices_by_floor)
        # assert False
    else:
        fight_data['picked'] = card_choices_by_floor[floor]['picked']
        fight_data['not_picked'] = card_choices_by_floor[floor]['not_picked']
        
    if master_data['current_hp_per_floor'] == 0:
        hp_change = battle_stat['damage']
    else:
        hp_change = master_data['current_hp_per_floor'][floor - 2] - master_data['current_hp_per_floor'][floor - 1]
    fight_data['damage_taken'] = hp_change
    return fight_data


def process_card_choice(card_choice_data, current_deck, current_relics):
    picked_card = card_choice_data['picked']
    if picked_card != 'SKIP' and picked_card != 'Singing Bowl':
        if 'Molten Egg 2' in current_relics and picked_card in game_constants.BASE_GAME_ATTACKS and picked_card[-2] != '+1':
            picked_card += '+1'
        if 'Toxic Egg 2' in current_relics and picked_card in game_constants.BASE_GAME_SKILLS and picked_card[-2] != '+1':
            picked_card += '+1'
        if 'Frozen Egg 2' in current_relics and picked_card in game_constants.BASE_GAME_POWERS and picked_card[-2] != '+1':
            picked_card += '+1'
        current_deck.append(picked_card)


def process_relics(relics, current_relics, master_relics, floor, unknowns, current_deck):
    for r in relics:
        obtain_relic(r, current_relics, master_relics, floor, unknowns, current_deck)


def process_campfire_choice(campfire_data, current_deck):
    choice = campfire_data['key']
    if choice == 'SMITH':
        upgrade_card(current_deck, campfire_data['data'])
    if choice == 'PURGE':
        current_deck.remove(campfire_data['data'])


def process_purchases(purchase_data, current_deck, current_relics, master_relics, floor, unknowns):
    purchased_cards = [x for x in purchase_data if x not in game_constants.BASE_GAME_RELICS and x not in game_constants.BASE_GAME_POTIONS]
    purchased_relics = [x for x in purchase_data if x not in purchased_cards and x not in game_constants.BASE_GAME_POTIONS]
    current_deck.extend(purchased_cards)
    for r in purchased_relics:
        obtain_relic(r, current_relics, master_relics, floor, unknowns, current_deck)


def process_purges(purge_data, current_deck):
    for card in purge_data:
        if card in current_deck:
            current_deck.remove(card)
        else:
            raise ValueError('process_purges: ' + card +  ' not in ' + str(current_deck))


def process_events(event_data, current_deck, current_relics, master_relics, floor, unknowns):
    if 'relics_obtained' in event_data:
        for r in event_data['relics_obtained']:
            obtain_relic(r, current_relics, master_relics, floor, unknowns, current_deck)
    if 'relics_lost' in event_data:
        for relic in event_data['relics_lost']:
            current_relics.remove(relic)
    if 'cards_obtained' in event_data:
        current_deck.extend(event_data['cards_obtained'])
    if 'cards_removed' in event_data:
        for card in event_data['cards_removed']:
            current_deck.remove(card)
    if 'cards_upgraded' in event_data:
        for card in event_data['cards_upgraded']:
            upgrade_card(current_deck, card)
    if 'event_name' in event_data and event_data['event_name'] == 'Vampires':
        current_deck[:] = [x for x in current_deck if not x.startswith('Strike')]


def process_neow(neow_bonus, neow_cost, current_deck, current_relics, master_relics, unknowns):
    unknown_removes_by_floor, unknown_upgrades_by_floor, unknown_transforms_by_floor, unknown_cards_by_floor = unknowns
    if neow_bonus == 'ONE_RARE_RELIC' or neow_bonus == 'RANDOM_COMMON_RELIC':
        current_relics.append(master_relics[1])
    elif neow_bonus == 'BOSS_RELIC':
        current_relics[0] = master_relics[0]
    elif neow_bonus == 'THREE_ENEMY_KILL':
        current_relics.append('NeowsBlessing')
    elif neow_bonus == 'UPGRADE_CARD':
        unknown_upgrades_by_floor[0] = [{'type': 'unknown'}]
    elif neow_bonus == 'REMOVE_CARD':
        unknown_removes_by_floor[0] = 1
    elif neow_bonus == 'REMOVE_TWO':
        unknown_removes_by_floor[0] = 2
    elif neow_bonus == 'TRANSFORM_CARD':
        unknown_transforms_by_floor[0] = 1
    elif neow_bonus == 'THREE_CARDS':
        unknown_cards_by_floor[0] = [{'type': 'unknown'}]
    elif neow_bonus == 'THREE_RARE_CARDS' or neow_bonus == 'ONE_RANDOM_RARE_CARD':
        unknown_cards_by_floor[0] = [{'type': 'rare'}]

    #if neow_cost == 'CURSE':
    #    unknown_cards_by_floor.get(0, []).append( {'type': 'curse'})
        
    #elif neow_bonus in ['TEN_PERCENT_HP_BONUS', 'HUNDRED_GOLD', 'TWO_FIFTY_GOLD', 'RANDOM_COLORLESS',
    #                    'TWENTY_PERCENT_HP_BONUS', 'RANDOM_COLORLESS_2']:
    #    pass
    #else:
    #    raise ValueError("unhandled neow bonus " + neow_bonus)


def upgrade_card(current_deck, card_to_upgrade):
    if card_to_upgrade not in current_deck:
        raise ValueError('upgrade_card: ' + card_to_upgrade + ' not in ' + str(current_deck))
    card_to_upgrade_index = current_deck.index(card_to_upgrade)
    # if 'earing' in card_to_upgrade:
        # print(f'Probably Searing Blow id: {card_to_upgrade}')
    current_deck[card_to_upgrade_index] += '+1'


def obtain_relic(relic_to_obtain, current_relics, master_relics, floor, unknowns, current_deck):
    unknown_removes_by_floor, unknown_upgrades_by_floor, unknown_transforms_by_floor, unknown_cards_by_floor = unknowns
    if relic_to_obtain == 'Black Blood':
        current_relics[0] = 'Black Blood'
        return
    if relic_to_obtain == 'Ring of the Serpent':
        current_relics[0] = 'Ring of the Serpent'
        return
    if relic_to_obtain == 'FrozenCore':
        current_relics[0] = 'FrozenCore'
        return
    #if relic_to_obtain == 'PureWater':
    #    current_relics[0] = 'PureWater'
    #    return
    
    if relic_to_obtain == 'Calling Bell':
        current_relics.extend(master_relics[len(current_relics) + 1:len(current_relics) + 4])
        current_deck.append('CurseOfTheBell')
    if relic_to_obtain == 'Necronomicon':
        current_deck.append('Necronomicurse')
    if relic_to_obtain == 'Empty Cage':
        unknown_removes_by_floor[floor] = 2
    if relic_to_obtain == 'Whetstone':
        unknown_upgrades_by_floor[floor] = [{'type': 'attack'}, {'type': 'attack'}]
    if relic_to_obtain == 'War Paint':
        unknown_upgrades_by_floor[floor] = [{'type': 'skill'}, {'type': 'skill'}]
        
    current_relics.append(relic_to_obtain)


def get_stats_by_floor_with_list(data, data_key):
    stats_by_floor = dict()
    if data_key in data:
        for stat in data[data_key]:
            floor = stat['floor']
            if floor not in stats_by_floor:
                stats_by_floor[floor] = list()
            stats_by_floor[floor].append(stat['key'])
    return stats_by_floor


def get_stat_with_separate_floor_list(data, obtain_key, floor_key):
    stats_by_floor = dict()
    if obtain_key in data and floor_key in data and len(data[obtain_key]) == len(data[floor_key]):
        obtains = data[obtain_key]
        floors = data[floor_key]
        for index, obt in enumerate(obtains):
            flr = floors[index]
            obt = obtains[index]
            if flr not in stats_by_floor:
                stats_by_floor[flr] = list()
            stats_by_floor[flr].append(obt)
    return stats_by_floor


def get_relics_by_floor(data):
    relics_by_floor = get_stats_by_floor_with_list(data, 'relics_obtained')
    boss_relics = data['boss_relics']
    if len(boss_relics) >= 1:
        picked_relic = boss_relics[0]['picked']
        if picked_relic != 'SKIP':
            relics_by_floor[17] = [picked_relic]
    if len(boss_relics) == 2:
        picked_relic = boss_relics[1]['picked']
        if picked_relic != 'SKIP':
            relics_by_floor[34] = [picked_relic]
    return relics_by_floor


def get_starting_relics(character):
    if character == 'IRONCLAD':
        return ['Burning Blood']
    elif character == 'THE_SILENT':
        return ['Ring of the Snake']
    elif character == 'DEFECT':
        return ['Cracked Core']
    elif character == 'WATCHER':
        return ['PureWater']
    else:
        print(f'Unsupported character {character}')


def get_starting_deck(character, ascension):
    basic_deck = ['Strike', 'Strike', 'Strike', 'Strike', 'Defend', 'Defend', 'Defend', 'Defend']
    if character == 'IRONCLAD':
        basic_deck.extend(['Strike', 'Bash'])
        character_spefic_basic_cards(basic_deck, '_R')
    elif character == 'THE_SILENT':
        basic_deck.extend(['Strike', 'Defend', 'Survivor', 'Neutralize'])
        character_spefic_basic_cards(basic_deck, '_G')
    elif character == 'DEFECT':
        basic_deck.extend(['Zap', 'Dualcast'])
        character_spefic_basic_cards(basic_deck, '_B')
    elif character == 'WATCHER':
        basic_deck.extend(['Eruption', 'Vigilance'])
        character_spefic_basic_cards(basic_deck, '_P')
    else:
        print(f'Unsupported character {character}')
    if ascension >= 10:
        basic_deck.append('AscendersBane')
    return basic_deck


def character_spefic_basic_cards(deck, suffix):
    for index, card in enumerate(deck):
        if card == 'Strike' or card == 'Defend':
            deck[index] = card + suffix


def resolve_missing_data(current_deck, current_relics, master_deck, master_relics, unknowns, master_data):
    unknown_removes_by_floor, unknown_upgrades_by_floor, unknown_transforms_by_floor, unknown_cards_by_floor = unknowns
    if current_deck != master_deck:
        if len(current_deck) > len(master_deck) and len(unknown_removes_by_floor) == 1 and len(unknown_upgrades_by_floor) == 0 and len(unknown_transforms_by_floor) == 0 and len(unknown_cards_by_floor) == 0:
            differences = list((Counter(current_deck) - Counter(master_deck)).elements())
            for floor, number_of_removes in unknown_removes_by_floor.items():
                if len(differences) == number_of_removes:
                    master_data['items_purged'].extend(differences)
                    for i in range(number_of_removes):
                        items_purched_floors = master_data['items_purged_floors']
                        items_purched_floors.append(floor)
                    return True, master_data
        elif len(current_deck) == len(master_deck) and len(unknown_upgrades_by_floor) == 1 and len(unknown_removes_by_floor) == 0 and len(unknown_transforms_by_floor) == 0 and len(unknown_cards_by_floor) == 0:
            diff1 = list((Counter(current_deck) - Counter(master_deck)).elements())
            diff2 = list((Counter(master_deck) - Counter(current_deck)).elements())
            if len(diff1) == len(diff2):
                upgraded_names_of_unupgraded_cards = [x + "+1" for x in diff1]
                if upgraded_names_of_unupgraded_cards == diff2:
                    for floor, upgrade_types in unknown_upgrades_by_floor.items():
                        if len(diff1) == len(upgrade_types):
                            for unupgraded_card in diff1:
                                master_data['campfire_choices'].append({"data": unupgraded_card, "floor": floor, "key": "SMITH"})
                            return True, master_data

    return False, None


BUILD_VERSION_REGEX = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}$')


def valid_build_number(string, character):
    pattern = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}$')
    if pattern.match(string):
        m = re.search('(.+)-(.+)-(.+)', string)
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))

        date = datetime.date(year, month, day)
        if date >= datetime.date(2020, 1, 16):
            return True
        elif character in ['IRONCLAD', 'THE_SILENT', 'DEFECT'] and date >= datetime.date(2019, 1, 23):
            return True

    return False


def is_bad_game_data(data):
    # Corrupted files
    necessary_fields = ['damage_taken', 'event_choices', 'card_choices', 'relics_obtained', 'campfire_choices',
                        'items_purchased', 'item_purchase_floors', 'items_purged', 'items_purged_floors',
                        'character_chosen', 'boss_relics', 'floor_reached', 'master_deck', 'relics']
    for field in necessary_fields:
        if field not in data:
            print(f'File missing field: {field}')
            return True

    # Modded runs
    key = 'character_chosen'
    if key not in data or data[key] not in ['IRONCLAD', 'THE_SILENT', 'DEFECT', 'WATCHER']:
        print(f'Modded character: {data[key]}')
        return True

    key = 'master_deck'
    if key not in data or set(data[key]).issubset(game_constants.BASE_GAME_CARDS_AND_UPGRADES) is False:
        deck = set(data[key])
        print(f'Modded file. Cards: {deck - game_constants.BASE_GAME_CARDS_AND_UPGRADES}')
        return True

    key = 'relics'
    if key not in data or set(data[key]).issubset(game_constants.BASE_GAME_RELICS) is False:
        return True

    # Watcher files since full release of watcher (v2.0) and ironclad, silent, defect since v1.0
    key = 'build_version'
    if key not in data or valid_build_number(data[key], data['character_chosen']) is False:
        return True

    
    # Non standard runs
    key = 'is_trial'
    if key not in data or data[key] is True:
        print('skipped because', key)
        return True

    key = 'is_daily'
    if key not in data or data[key] is True:
        print('skipped because', key)
        return True

    key = 'daily_mods'
    if key in data:
        print('skipped because', key)
        return True

    key = 'chose_seed'
    if key not in data or data[key] is True:
        print('skipped because', key)
        return True

    # Endless mode
    key = 'is_endless'
    if key not in data or data[key] is True:
        print('skipped because', key)
        return True

    key = 'circlet_count'
    if key not in data or data[key] > 0:
        print('skipped because', key)
        return True

    key = 'floor_reached'
    if key not in data or data[key] > 60:
        print('skipped because', key)
        return True

    # Really bad players or give ups
    key = 'floor_reached'
    if key not in data or data[key] < 4:
        print('skipped because', key)
        return True

    key = 'score'
    if key not in data or data[key] < 10:
        print('skipped because', key)
        return True

    key = 'player_experience'
    if key not in data or data[key] < 100:
        print('skipped because', key)
        return True


def write_file(data, name):
    with open(name, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def process_single_game(data_dir, filename):
    
    with gzip.open(f"{data_dir}/{filename}", 'r') as file_contents:
        success_count = 0
        for game_data in json.load(file_contents):
            try:
                game_data = game_data['event']
                result = process_run(game_data, ProcessingProblemsSummary())
                if result is not None:
                    pprint.pprint(game_data)
                    pprint.pprint(result)
                    print('\n\n')
                    success_count += 1
                    if success_count > 3:
                        break
            except RuntimeError as e:
                print('skipping game ' + str(e))
                continue
            except ValueError as e:
                print('skipping game ' + str(e))
                continue

if __name__ == '__main__':
    directory = 'raw_logs'
    process_runs(directory)
    # process_single_game('raw_logs/Monthly_2020_11', '2020-11-04-03-59#1382.json.gz')
    

"""
# Keys

gold_per_floor
floor_reached
playtime
items_purged
score
play_id
local_time
is_ascension_mode
campfire_choices
neow_cost
seed_source_timestamp
circlet_count
master_deck
relics
potions_floor_usage
damage_taken
seed_played
potions_obtained
is_trial
path_per_floor
character_chosen
items_purchased
campfire_rested
item_purchase_floors
current_hp_per_floor
gold
neow_bonus
is_prod
is_daily
chose_seed
campfire_upgraded
win_rate
timestamp
path_taken
build_version
purchased_purges
victory
max_hp_per_floor
card_choices
player_experience
relics_obtained
event_choices
is_beta
boss_relics
items_purged_floors
is_endless
potions_floor_spawned
killed_by
ascension_level
"""
