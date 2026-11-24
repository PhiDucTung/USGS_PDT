% Run before fillMissingFlows.m. Retain leading zeroes with string HUCs.
opts = detectImportOptions('grid_huc_output/grid_huc.csv');
opts = setvartype(opts, {'grid_id','huc8','huc6','huc4','match_status','candidate_huc8'}, 'string');
gridHUCs = readtable('grid_huc_output/grid_huc.csv', opts);
assert(~any(ismissing(gridHUCs.huc8)), 'Resolve unmatched/ambiguous HUCs first.');
% stationHUCs.huc4/huc6/huc8 must ALSO be strings of width 4/6/8.
% Do not cast HUCs to numeric; leading zeroes carry meaning.
save('gridHUCs.mat', 'gridHUCs');
