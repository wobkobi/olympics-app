import React, { createContext, useContext, ReactNode } from "react";

interface ResetContextType {
  resetFiltersAndTable: () => void;
}

export const ResetContext = createContext<ResetContextType | undefined>(undefined);

export const ResetProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const resetFiltersAndTable = () => {
    // You can implement the reset logic here if needed
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
