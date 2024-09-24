import React, { createContext, useState, useContext, ReactNode } from "react";

interface ResetContextType {
  resetFiltersAndTable: () => void;
}

export const ResetContext = createContext<ResetContextType | undefined>(undefined);

export const ResetProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [reset, setReset] = useState(false);

  const resetFiltersAndTable = () => {
    setReset(true);
  };

  return (
    <ResetContext.Provider value={{ resetFiltersAndTable }}>
      {children}
    </ResetContext.Provider>
  );
};

export const useResetContext = () => {
  const context = useContext(ResetContext);
  if (!context) {
    throw new Error("useResetContext must be used within a ResetProvider");
  }
  return context;
};
