import React from "react";

interface FilterMenuProps {
  searchQuery: string;
  handleSearch: (e: React.ChangeEvent<HTMLInputElement>) => void;
  selectedOlympics: string;
  handleOlympicsChange: (game: string) => void;
  summerGames: Array<{ year: string; season: string; file: string }>;
  winterGames: Array<{ year: string; season: string; file: string }>;
  selectedRole: string;
  setSelectedRole: (role: string) => void;
  rolesList: string[];
  visibleColumns: string[];
  handleColumnToggle: (column: string) => void;
  isFilterMenuOpen: boolean;
  setIsFilterMenuOpen: (open: boolean) => void;
}

const FilterMenu: React.FC<FilterMenuProps> = ({
  searchQuery,
  handleSearch,
  selectedOlympics,
  handleOlympicsChange,
  summerGames,
  winterGames,
  selectedRole,
  setSelectedRole,
  rolesList,
  visibleColumns,
  handleColumnToggle,
  setIsFilterMenuOpen,
}) => {
  return (
    <div className="fixed top-0 right-0 w-64 h-full bg-white shadow-lg p-4 z-40">
      <h2 className="text-xl mb-4">Filters</h2>
      
      {/* Search Filter */}
      <div className="mb-4">
        <label htmlFor="search" className="block text-sm font-medium text-gray-700 mb-2">
          Search by Name
        </label>
        <input
          type="text"
          id="search"
          value={searchQuery}
          onChange={handleSearch}
          className="w-full p-2 border border-gray-300 rounded"
          placeholder="Enter athlete's name"
        />
      </div>

      {/* Olympic Games Filter */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Select Olympic Games
        </label>
        <select
          value={selectedOlympics}
          onChange={(e) => handleOlympicsChange(e.target.value)}
          className="w-full p-2 border border-gray-300 rounded"
        >
          <option value="">All Games</option>
          {summerGames.map((game) => (
            <option key={game.year} value={`${game.year} ${game.season} Olympics`}>
              {game.year} {game.season}
            </option>
          ))}
          {winterGames.map((game) => (
            <option key={game.year} value={`${game.year} ${game.season} Olympics`}>
              {game.year} {game.season}
            </option>
          ))}
        </select>
      </div>

      {/* Role Filter */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Select Role
        </label>
        <select
          value={selectedRole}
          onChange={(e) => setSelectedRole(e.target.value)}
          className="w-full p-2 border border-gray-300 rounded"
        >
          <option value="">All Roles</option>
          {rolesList.map((role) => (
            <option key={role} value={role}>
              {role}
            </option>
          ))}
        </select>
      </div>

      {/* Column Visibility Toggle */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Toggle Columns
        </label>
        {["name", "sport", "event", "position"].map((column) => (
          <div key={column} className="flex items-center mb-2">
            <input
              type="checkbox"
              id={`column-${column}`}
              checked={visibleColumns.includes(column)}
              onChange={() => handleColumnToggle(column)}
              className="h-4 w-4 text-olympicBlue border-gray-300 rounded"
            />
            <label htmlFor={`column-${column}`} className="ml-2 text-sm text-gray-700">
              {capitalizeFirstLetter(column)}
            </label>
          </div>
        ))}
      </div>

      {/* Close Filter Menu Button */}
      <button
        onClick={() => setIsFilterMenuOpen(false)}
        className="mt-4 w-full p-2 bg-red-500 text-white rounded hover:bg-red-600"
      >
        Close Filters
      </button>
    </div>
  );
};

// Helper function to capitalize first letter
const capitalizeFirstLetter = (str: string) => {
  return str.charAt(0).toUpperCase() + str.slice(1);
};

export default FilterMenu;
